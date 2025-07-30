#pragma once  
#include <QtWidgets/QMainWindow>  
#include "ui_StatFromDB.h"
#include<QFile>

class StatFromDB : public QMainWindow
{
    Q_OBJECT

public:
    StatFromDB(QWidget* parent = nullptr);
    ~StatFromDB();

private:
    Ui::StatFromDBClass ui;
    bool dbConnected = false;
    bool answerImported = false;
    bool hasQueried = false;
    void connectToDatabase(const QString& host, int port, const QString& dbName, const QString& user, const QString& pwd);
    void fillSchemaComboBox();
    void excuteSQL(QFile& file);


private slots:
    void onQuerryButtonClicked();
    void onTimeQuerryButtonClicked();
    void onSelectAnswerClicked();
    void onInsertAnswerClicked();
    void onConnectDbButtonClicked();
    void onExportButtonClicked();
};
