#include "StatFromDB.h"
#include <QtWidgets/QApplication>

int main(int argc, char *argv[])
{
    QApplication app(argc, argv);
    StatFromDB window;
    window.show();
    return app.exec();
}
