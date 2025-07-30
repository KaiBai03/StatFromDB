#include "StatFromDB.h"
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QDebug>
#include <QSqlError>
#include <QFile>
#include <QTextStream>
#include <QDateTime>
#include <QPushButton>
#include<QFileDialog>
#include<QProcess>
#include<QCoreApplication>
#include<QMessageBox>
#include<QSettings>

StatFromDB::StatFromDB(QWidget *parent)
    : QMainWindow(parent)
{	

    ui.setupUi(this);
    ui.s_judgetime_stackedWidget->setCurrentIndex(0);
    ui.s_sendtime_stackedWidget->setCurrentIndex(0);
    QStringList pageNames = { "40gal", "80gal", "120gal" };

    // s_judgetime 左右按钮
    connect(ui.s_judgetime_l, &QPushButton::clicked, this, [this, pageNames]() {
        int count = ui.s_judgetime_stackedWidget->count();
        int idx = (ui.s_judgetime_stackedWidget->currentIndex() - 1 + count) % count;
        ui.s_judgetime_stackedWidget->setCurrentIndex(idx);
		ui.s_judgetime_label->setText(pageNames[idx]);
    });
    connect(ui.s_judgetime_r, &QPushButton::clicked, this, [this, pageNames]() {
        int count = ui.s_judgetime_stackedWidget->count();
        int idx = (ui.s_judgetime_stackedWidget->currentIndex() + 1) % count;
        ui.s_judgetime_stackedWidget->setCurrentIndex(idx);
        ui.s_judgetime_label->setText(pageNames[idx]);
    });

    // s_sendtime 左右按钮
    connect(ui.s_sendtime_l, &QPushButton::clicked, this, [this, pageNames]() {
        int count = ui.s_sendtime_stackedWidget->count();
        int idx = (ui.s_sendtime_stackedWidget->currentIndex() - 1 + count) % count;
        ui.s_sendtime_stackedWidget->setCurrentIndex(idx);
		ui.s_sendtime_label->setText(pageNames[idx]);
    });
    connect(ui.s_sendtime_r, &QPushButton::clicked, this, [this, pageNames]() {
        int count = ui.s_sendtime_stackedWidget->count();
        int idx = (ui.s_sendtime_stackedWidget->currentIndex() + 1) % count;
        ui.s_sendtime_stackedWidget->setCurrentIndex(idx);
		ui.s_sendtime_label->setText(pageNames[idx]);
    });


    connect(ui.queryButton, &QPushButton::clicked, this, &StatFromDB::onQuerryButtonClicked);
	connect(ui.timeQueryButton, &QPushButton::clicked, this, &StatFromDB::onTimeQuerryButtonClicked);
    connect(ui.selectAnswerButton, &QPushButton::clicked, this, &StatFromDB::onSelectAnswerClicked);
    connect(ui.insertAnswerButton, &QPushButton::clicked, this, &StatFromDB::onInsertAnswerClicked);
	connect(ui.connectDbButton, &QPushButton::clicked, this, &StatFromDB::onConnectDbButtonClicked);
    connect(ui.exportButton, &QPushButton::clicked, this, &StatFromDB::onExportButtonClicked);
}

void StatFromDB::onConnectDbButtonClicked() {
    QString host = ui.dbHostEdit->text().trimmed();
    QString portStr = ui.dbPortEdit->text().trimmed();
    QString dbName = ui.dbNameEdit->text().trimmed();
    QString user = ui.dbUserEdit->text().trimmed();
    QString pwd = ui.dbPwdEdit->text().trimmed();

    bool allEmpty = host.isEmpty() && portStr.isEmpty() && dbName.isEmpty() && user.isEmpty() && pwd.isEmpty();
        bool allFilled = !host.isEmpty() && !portStr.isEmpty() && !dbName.isEmpty() && !user.isEmpty() && !pwd.isEmpty();

    if (allEmpty) {
        // 读取配置文件
        QSettings settings("config.ini", QSettings::IniFormat);
        settings.beginGroup("database");
        host = settings.value("host").toString();
        int port = settings.value("port").toInt();
        dbName = settings.value("dbname").toString();
        user = settings.value("user").toString();
        pwd = settings.value("password").toString();
        settings.endGroup();
        connectToDatabase(host, port, dbName, user, pwd);
    }
    else if (allFilled) {
        int port = portStr.toInt();
        connectToDatabase(host, port, dbName, user, pwd);
    }
    else {
        QMessageBox::warning(this, "错误", "请将所有数据库信息填写完整，或全部留空以使用配置文件。");
    }
}

void StatFromDB::connectToDatabase(const QString& host, int port, const QString& dbName, const QString& user, const QString& pwd)
{
    QSqlDatabase db = QSqlDatabase::addDatabase("QPSQL");
    db.setHostName(host);
    db.setPort(port);
    db.setDatabaseName(dbName);
    db.setUserName(user);
    db.setPassword(pwd);
    db.setConnectOptions("client_encoding=UTF8");

    if (!db.open()) {
        qDebug() << "connect failed";
        QMessageBox::critical(this, "连接失败", "无法连接到数据库，请检查参数。");
    }
    else {
        qDebug() << "connect succeeded";
		QMessageBox::information(this, "连接成功", "成功连接到数据库！");
        fillSchemaComboBox();
        QString schema = ui.dbSchemaComboBox->currentText();
        QString searchPath = (schema == "public") ? "public" : QString("%1, public").arg(schema);
        QSqlQuery query;
		query.exec(QString("SET search_path TO %1").arg(searchPath));
        dbConnected = true;
        answerImported = false; // 连接数据库后需重新导入标准答案
        hasQueried = false;
    }
}

void StatFromDB::fillSchemaComboBox()
{
    ui.dbSchemaComboBox->clear();
    QSqlQuery query(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name NOT IN ('pg_catalog', 'information_schema','pg_toast') "
        "ORDER BY schema_name;"
    );
    while (query.next()) {
        ui.dbSchemaComboBox->addItem(query.value(0).toString());
    }
}

void StatFromDB::onSelectAnswerClicked()
{
    QString fileName = QFileDialog::getOpenFileName(this, "选择标准答案", "", "所有文件 (*.*)");
    if (!fileName.isEmpty()) {
        ui.answerFileTextEdit->setPlainText(fileName);
    }
}

void StatFromDB::onInsertAnswerClicked() {
    if (!dbConnected) {
        QMessageBox::warning(this, "错误", "请先连接到数据库！");
        return;
    }
    QString csvPath = ui.answerFileTextEdit->toPlainText();
    QString exeDir = QCoreApplication::applicationDirPath();
    QString scriptPath = QDir(exeDir).filePath("InsertStandAnswerToDb.exe");
    QStringList args;
    args << csvPath;
    bool allFilled = !ui.dbHostEdit->text().trimmed().isEmpty()
        && !ui.dbPortEdit->text().trimmed().isEmpty()
        && !ui.dbNameEdit->text().trimmed().isEmpty()
        && !ui.dbUserEdit->text().trimmed().isEmpty()
        && !ui.dbPwdEdit->text().trimmed().isEmpty();
    if (allFilled) {
        args << "--host" << ui.dbHostEdit->text().trimmed()
            << "--port" << ui.dbPortEdit->text().trimmed()
            << "--dbname" << ui.dbNameEdit->text().trimmed()
            << "--user" << ui.dbUserEdit->text().trimmed()
            << "--password" << ui.dbPwdEdit->text().trimmed();
    }
    QProcess process;
    process.start(scriptPath, args);
    if (!process.waitForStarted()) {
        QMessageBox::critical(this, "错误", "无法启动导入进程！");
        return;
    }
    if (!process.waitForFinished(-1)) {
        QMessageBox::critical(this, "错误", "导入进程未正常结束！");
        return;
    }
    QByteArray stdErr = process.readAllStandardError();
    QByteArray stdOut = process.readAllStandardOutput();
    int exitCode = process.exitCode();
    if (exitCode == 0) {
        QMessageBox::information(this, "Success", "成功导入标准答案");
        answerImported = true;
        hasQueried = false;
    }
    else {
        // 显示详细错误信息
        QMessageBox::critical(this, "导入标准答案失败",
            QString("导入失败，请检查文件路径和内容。\n\n标准输出:\n%1\n\n标准错误:\n%2")
            .arg(QString::fromLocal8Bit(stdOut))
            .arg(QString::fromLocal8Bit(stdErr)));
        answerImported = false;
    }
}
void StatFromDB::onTimeQuerryButtonClicked() {
    if(!dbConnected) {
        QMessageBox::warning(this, "错误", "请先连接到数据库！");
        return;
	}
    for (QObject* obj : this->findChildren<QLineEdit*>()) {
        if (obj == ui.dbHostEdit || obj == ui.dbUserEdit || obj==ui.dbNameEdit||obj==ui.dbPortEdit||obj==ui.dbPwdEdit) continue; // 保留不清空的
        static_cast<QLineEdit*>(obj)->clear();
    }

    QDateTime startTime = ui.startDateTimeEdit->dateTime();
    QDateTime endTime = ui.endDateTimeEdit->dateTime();
    QString startStr = startTime.toString("yyyy-MM-dd HH:mm:ss.zzz");
    QString endStr = endTime.toString("yyyy-MM-dd HH:mm:ss.zzz");

	QFile idMatch(":/StatFromDB/sql/sql/idMatchedTables.sql");
	QFile timeFilterR(":/StatFromDB/sql/timeFilteredResult.sql");
	excuteSQL(idMatch);
    QSqlQuery query;
    query.exec("DROP TABLE IF EXISTS public.id_matched_p_filtered_by_time;");
    query.exec("DROP TABLE IF EXISTS public.id_matched_s_filtered_by_time;");
    query.exec(QString("CREATE TABLE public.id_matched_p_filtered_by_time AS "
                       "SELECT * FROM public.id_matched_p_wave_info WHERE jk_time >= '%1' AND jk_time <= '%2';")
		.arg(startStr, endStr));
    query.exec(QString("CREATE TABLE public.id_matched_s_filtered_by_time AS "
                       "SELECT * FROM public.id_matched_s_wave_info WHERE jk_time >= '%1' AND jk_time <= '%2';")
        .arg(startStr, endStr)); 

	excuteSQL(timeFilterR);

    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.time_filtered_p_send_time;")) {
        qDebug() << "time_filtered_p_send_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.P_send_sum->setText(query.value(0).toString());
            ui.P_send_le_100->setText(query.value(1).toString());
            ui.P_send_le_rate->setText(query.value(2).toString());
            ui.P_send_pass->setText(query.value(3).toString());
            ui.P_send_avg_t->setText(query.value(4).toString());
            ui.P_send_max_t->setText(query.value(5).toString());
            ui.P_send_min_t->setText(query.value(6).toString());
        }
    }
    //s_40gal_send_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.time_filtered_s_40gal_send_time;")) {
        qDebug() << "time_filtered_s_40gal_send_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.s_40gal_sendtime_sum->setText(query.value(0).toString());
            ui.s_40gal_sendtime_le_cnt->setText(query.value(1).toString());
            ui.s_40gal_sendtime_le_rate->setText(query.value(2).toString());
            ui.s_40gal_sendtime_pass->setText(query.value(3).toString());
            ui.s_40gal_sendtime_avg->setText(query.value(4).toString());
            ui.s_40gal_sendtime_max->setText(query.value(5).toString());
            ui.s_40gal_sendtime_min->setText(query.value(6).toString());
        }
    }

    //s_80gal_send_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.time_filtered_s_80gal_send_time;")) {
        qDebug() << "time_filtered_s_80gal_send_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.s_80gal_sendtime_sum->setText(query.value(0).toString());
            ui.s_80gal_sendtime_le_cnt->setText(query.value(1).toString());
            ui.s_80gal_sendtime_le_rate->setText(query.value(2).toString());
            ui.s_80gal_sendtime_pass->setText(query.value(3).toString());
            ui.s_80gal_sendtime_avg->setText(query.value(4).toString());
            ui.s_80gal_sendtime_max->setText(query.value(5).toString());
            ui.s_80gal_sendtime_min->setText(query.value(6).toString());
        }
    }

    //s_120gal_send_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.time_filtered_s_120gal_send_time;")) {
        qDebug() << "time_filtered_s_120gal_send_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.s_120gal_sendtime_sum->setText(query.value(0).toString());
            ui.s_120gal_sendtime_le_cnt->setText(query.value(1).toString());
            ui.s_120gal_sendtime_le_rate->setText(query.value(2).toString());
            ui.s_120gal_sendtime_pass->setText(query.value(3).toString());
            ui.s_120gal_sendtime_avg->setText(query.value(4).toString());
            ui.s_120gal_sendtime_max->setText(query.value(5).toString());
            ui.s_120gal_sendtime_min->setText(query.value(6).toString());
        }
    }


}
void StatFromDB::onQuerryButtonClicked()
{
    if (!dbConnected) {
        QMessageBox::warning(this, "错误", "请先连接到数据库！");
        return;
    }
    if (!answerImported) {
        QMessageBox::warning(this, "错误", "请先导入标准答案！");
        return;
    }
    QSqlQuery query;

	QFile waveinfo(":/StatFromDB/sql/waveInfoTables.sql");
	QFile summary(":/StatFromDB/sql/summaryTables.sql");
	QFile details(":/StatFromDB/sql/detailsTables.sql");
	excuteSQL(waveinfo);
	excuteSQL(summary);
	excuteSQL(details);
	//p_send_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.p_send_time;")) {
		qDebug() << "p_send_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.P_send_sum->setText(query.value(0).toString());
            ui.P_send_le_100->setText(query.value(1).toString());
            ui.P_send_le_rate->setText(query.value(2).toString());
            ui.P_send_pass->setText(query.value(3).toString());
            ui.P_send_avg_t->setText(query.value(4).toString());
            ui.P_send_max_t->setText(query.value(5).toString());
            ui.P_send_min_t->setText(query.value(6).toString());
        }
    }


    //p_epcicenter_deviation
    if (!query.exec("SELECT 总数,\"偏差≤60km组数\",\"偏差≤60km占比\",\"是否达标(60km)\",\"偏差≤100km组数\",\"偏差≤100km占比\",\"是否达标(100km)\",偏差最大值,最大值不超过300km FROM public.p_epicenter_deviation;")) {
		qDebug() << "p_epcicenter_deviation查询失败: " << query.lastError().text();
        }
    else {
        if (query.next()) {
		    ui.single_p_dis_diff_sum->setText(query.value(0).toString());
		    ui.diff_le_60km_cnt->setText(query.value(1).toString());
		    ui.diff_le_60km_rate->setText(query.value(2).toString());
		    ui.diff_le_60km_pass->setText(query.value(3).toString());
		    ui.diff_le_100km_cnt->setText(query.value(4).toString());
		    ui.diff_le_100km_rate->setText(query.value(5).toString());
            ui.diff_le_100km_pass->setText(query.value(6).toString());
		    ui.single_p_dis_diff_max->setText(query.value(7).toString());
		    ui.dis_diff_max_le_300km_pass->setText(query.value(8).toString());
        }
    }

	//p_judge_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值 FROM public.p_judge_time;")) {
		qDebug() << "p_judge_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
		    ui.single_p_judgetime_sum->setText(query.value(0).toString());
		    ui.single_p_judgetime_le_3->setText(query.value(1).toString());
		    ui.single_p_judgetime_le_3_rate->setText(query.value(2).toString());
		    ui.single_p_judgetime_pass->setText(query.value(3).toString());
		    ui.single_p_judgetime_avg->setText(query.value(4).toString());
        }
    }

    //p_mag_deviation
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,偏差平均值 FROM public.p_mag_deviation")) {
		qDebug() << "p_mag_deviation查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
		    ui.single_p_mag_diff_sum->setText(query.value(0).toString());
		    ui.single_p_mag_diff_le_1->setText(query.value(1).toString());
		    ui.single_p_mag_diff_le_rate->setText(query.value(2).toString());
		    ui.single_p_mag_diff_pass->setText(query.value(3).toString());
		    ui.single_p_mag_diff_avg->setText(query.value(4).toString());
        }
    }

    //p_warning_miss
    if (!query.exec("SELECT 总数,漏报数,漏报率,是否达标 FROM public.p_warning_miss;")) {
        qDebug() << "p_warning_miss查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.single_p_warning_miss_sum->setText(query.value(0).toString());
            ui.single_p_warning_miss_cnt->setText(query.value(1).toString());
            ui.single_p_warning_miss_rate->setText(query.value(2).toString());
		    ui.single_p_warning_miss_pass->setText(query.value(3).toString());
        }
    }

    //s_alarm_before_p
    if (!query.exec("SELECT 先报警后预警数量 FROM public.s_alarm_before_p;")) {
		qDebug() << "s_alarm_before_p查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
		    ui.s_alarm_before_p_alarm_cnt->setText(query.value(0).toString());
        }
    }

    //s_40gal_send_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.s_40gal_send_time;")) {
		qDebug() << "s_40gal_send_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
		    ui.s_40gal_sendtime_sum->setText(query.value(0).toString());
		    ui.s_40gal_sendtime_le_cnt->setText(query.value(1).toString());
		    ui.s_40gal_sendtime_le_rate->setText(query.value(2).toString());
		    ui.s_40gal_sendtime_pass->setText(query.value(3).toString());
		    ui.s_40gal_sendtime_avg->setText(query.value(4).toString());
		    ui.s_40gal_sendtime_max->setText(query.value(5).toString());
		    ui.s_40gal_sendtime_min->setText(query.value(6).toString());
        }
    }

    //s_80gal_send_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.s_80gal_send_time;")) {
        qDebug() << "s_80gal_send_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.s_80gal_sendtime_sum->setText(query.value(0).toString());
            ui.s_80gal_sendtime_le_cnt->setText(query.value(1).toString());
            ui.s_80gal_sendtime_le_rate->setText(query.value(2).toString());
            ui.s_80gal_sendtime_pass->setText(query.value(3).toString());
            ui.s_80gal_sendtime_avg->setText(query.value(4).toString());
            ui.s_80gal_sendtime_max->setText(query.value(5).toString());
            ui.s_80gal_sendtime_min->setText(query.value(6).toString());
        }
    }

    //s_120gal_send_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.s_120gal_send_time;")) {
        qDebug() << "s_120gal_send_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.s_120gal_sendtime_sum->setText(query.value(0).toString());
            ui.s_120gal_sendtime_le_cnt->setText(query.value(1).toString());
            ui.s_120gal_sendtime_le_rate->setText(query.value(2).toString());
            ui.s_120gal_sendtime_pass->setText(query.value(3).toString());
            ui.s_120gal_sendtime_avg->setText(query.value(4).toString());
            ui.s_120gal_sendtime_max->setText(query.value(5).toString());
            ui.s_120gal_sendtime_min->setText(query.value(6).toString());
        }
    }

    //s_warning_miss
    if(!query.exec("SELECT 总数,漏报数,漏报率,是否达标 FROM public.s_warning_miss;")) {
        qDebug() << "s_warning_miss查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.s_warning_miss_sum->setText(query.value(0).toString());
            ui.s_warning_miss_cnt->setText(query.value(1).toString());
		    ui.s_warning_miss_rate->setText(query.value(2).toString());
            ui.s_warning_miss_pass->setText(query.value(3).toString());
        }
	}

    //s_peak_deviation
    if(!query.exec("SELECT 总数,合格数,合格率,是否达标,最大偏差 FROM public.s_peak_deviation;")) {
        qDebug() << "s_peak_deviation查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.s_peak_deviation_sum->setText(query.value(0).toString());
            ui.s_peak_deviation_le_cnt->setText(query.value(1).toString());
            ui.s_peak_deviation_le_rate->setText(query.value(2).toString());
            ui.s_peak_deviation_pass->setText(query.value(3).toString());
            ui.s_peak_deviation_max->setText(query.value(4).toString());
        }
	}

	//s_40gal_judge_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.s_40gal_judge_time;")) {
		qDebug() << "s_40gal_judge_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
		    ui.s_40gal_judgetime_sum->setText(query.value(0).toString());
		    ui.s_40gal_judgetime_le_cnt->setText(query.value(1).toString());
		    ui.s_40gal_judgetime_le_rate->setText(query.value(2).toString());
		    ui.s_40gal_judgetime_pass->setText(query.value(3).toString());
		    ui.s_40gal_judgetime_avg->setText(query.value(4).toString());
		    ui.s_40gal_judgetime_max->setText(query.value(5).toString());
		    ui.s_40gal_judgetime_min->setText(query.value(6).toString());
        }
    }

    //s_80gal_judge_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.s_80gal_judge_time;")) {
        qDebug() << "s_80gal_judge_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.s_80gal_judgetime_sum->setText(query.value(0).toString());
            ui.s_80gal_judgetime_le_cnt->setText(query.value(1).toString());
            ui.s_80gal_judgetime_le_rate->setText(query.value(2).toString());
            ui.s_80gal_judgetime_pass->setText(query.value(3).toString());
            ui.s_80gal_judgetime_avg->setText(query.value(4).toString());
            ui.s_80gal_judgetime_max->setText(query.value(5).toString());
            ui.s_80gal_judgetime_min->setText(query.value(6).toString());
        }
    }

    //s_120gal_judge_time
    if (!query.exec("SELECT 总数,合格数,合格率,是否达标,平均值,最大值,最小值 FROM public.s_120gal_judge_time;")) {
        qDebug() << "s_120gal_judge_time查询失败: " << query.lastError().text();
    }
    else {
        if (query.next()) {
            ui.s_120gal_judgetime_sum->setText(query.value(0).toString());
            ui.s_120gal_judgetime_le_cnt->setText(query.value(1).toString());
            ui.s_120gal_judgetime_le_rate->setText(query.value(2).toString());
            ui.s_120gal_judgetime_pass->setText(query.value(3).toString());
            ui.s_120gal_judgetime_avg->setText(query.value(4).toString());
            ui.s_120gal_judgetime_max->setText(query.value(5).toString());
            ui.s_120gal_judgetime_min->setText(query.value(6).toString());
        }
    }

    hasQueried = true;
}



void StatFromDB::onExportButtonClicked()
{
    if (!dbConnected) {
        QMessageBox::warning(this, "错误", "请先连接到数据库！");
        return;
    }
    if (!answerImported) {
        QMessageBox::warning(this, "错误", "请先导入标准答案！");
        return;
    }
    if (!hasQueried) {
        QMessageBox::warning(this, "提示", "请先点击查询按钮，确保导出的是最新统计结果！");
        return;
    }
    QString exeDir = QCoreApplication::applicationDirPath();
    QString scriptPath = QDir(exeDir).filePath("ExportResultToExcel.exe");

    QStringList args;
    QString host = ui.dbHostEdit->text().trimmed();
    QString port = ui.dbPortEdit->text().trimmed();
    QString dbName = ui.dbNameEdit->text().trimmed();
    QString user = ui.dbUserEdit->text().trimmed();
    QString pwd = ui.dbPwdEdit->text().trimmed();

    bool allEmpty = host.isEmpty() && port.isEmpty() && dbName.isEmpty() && user.isEmpty() && pwd.isEmpty();
    bool allFilled = !host.isEmpty() && !port.isEmpty() && !dbName.isEmpty() && !user.isEmpty() && !pwd.isEmpty();

    if (!allEmpty && !allFilled) {
        QMessageBox::warning(this, "错误", "请将所有数据库信息填写完整，或全部留空以使用配置文件。");
        return;
    }

    if (allFilled) {
        args << "--host" << host
            << "--port" << port
            << "--dbname" << dbName
            << "--user" << user
            << "--password" << pwd;
    }

    QProcess process;
    process.setWorkingDirectory(QCoreApplication::applicationDirPath());
    process.start(scriptPath,args);
    if (!process.waitForStarted()) {
        qDebug() << "Failed to start Python process";
        QMessageBox::critical(this, "错误", "无法启动Python进程！");
        return;
    }
    if (!process.waitForFinished(-1)) {
        qDebug() << "Python process did not finish";
        QMessageBox::critical(this, "错误", "Python进程未正常结束！");
        return;
    }

    int exitCode = process.exitCode();
    if (exitCode == 0) {
        // 获取导出目录
        QString exportDir = QCoreApplication::applicationDirPath();
        QMessageBox::information(this, "导出完成",
            QString("统计结果和数据明细已成功导出。\n\n导出目录：\n%1").arg(exportDir));
    }
    else {
        QMessageBox::critical(this, "导出失败", "导出失败，请先点击查询统计数据并检查数据库连接配置。");
    }
}

//执行SQL文件中的所有语句
//void StatFromDB::excuteSQL(QFile& file) {
//    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
//        qDebug() << "错误,无法打开SQL文件:"<<file.fileName();
//        return;
//    }
//    QTextStream in(&file);
//    QString sql = in.readAll();
//    file.close();
//    QStringList statements = sql.split(';', Qt::SkipEmptyParts);
//    QSqlQuery query;
//    for (const QString& statement : statements) {
//        QString trimmed = statement.trimmed();
//        if (!trimmed.isEmpty()) {
//            if (!query.exec(trimmed)) {
//                qDebug() << "错误信息: " << query.lastError().text();
//                return;
//            }
//        }
//    }
//}

void StatFromDB::excuteSQL(QFile& file) {
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        qDebug() << "错误,无法打开SQL文件:" << file.fileName();
        return;
    }
    QTextStream in(&file);
    QString sql = in.readAll();
    file.close();
    QStringList statements = sql.split(';', Qt::SkipEmptyParts);
    QSqlQuery query;
    for (const QString& statement : statements) {
        QString trimmed = statement.trimmed();
        if (!trimmed.isEmpty()) {
            if (!query.exec(trimmed)) {
                qDebug() << "错误信息: " << query.lastError().text();
                // 不要 return，继续执行后续语句
            }
        }
    }
}



StatFromDB::~StatFromDB()
{
}
