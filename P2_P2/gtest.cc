#include <string.h>
#include <gtest/gtest.h>
#include "test.h"
#include "Utilities.h"
#include "BigQ.h"

// -------------------------------------------------------------------------------------------------------------
class TestHelper{
    public:
    const std::string dbfile_dir = "dbfiles/"; 
    const std::string tpch_dir ="/home/mk/Documents/uf docs/sem 2/Database Implementation/git/tpch-dbgen/"; 
    const std::string catalog_path = "catalog";
    static OrderMaker sortorder;
    void get_sort_order (OrderMaker &sortorder) {
        cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
        if (yyparse() != 0) {
            cout << "Can't parse your sort CNF.\n";
            exit (1);
        }
        cout << " \n";
        Record literal;
        CNF sort_pred;
        sort_pred.GrowFromParseTree (final, new Schema ("catalog","nation"), literal);
        OrderMaker dummy;
        sort_pred.GetSortOrders (sortorder, dummy);
    }
    void deleteFile(string filePath) {
             if(Utilities::checkfileExist(filePath.c_str())) {
                if( remove(filePath.c_str())!= 0) {
                    cerr<< "Error deleting file";   
                }
            }
    }
};
// -------------------------------------------------------------------------------------------------------------
TEST(testingLoadSortedFile,testingLoadSortedFile) {
        TestHelper GTestHelperObject;
        OrderMaker sortorder;string tpchpath(tpch_dir);
        Schema nation ("catalog", "nation");
        DBFile sortedDBFile;Record tempRecord;
        const string schemaSuffix = "nation.bin";
        const string dbFilePath = GTestHelperObject.dbfile_dir + schemaSuffix;

        // create new DBFile of type 'sorted'
        if(!Utilities::checkfileExist(dbFilePath.c_str())) {
            sortedDBFile.Create(dbFilePath.c_str(),sorted,NULL);
        }

        // open dbfile
        sortedDBFile.Open(dbFilePath.c_str());
        sortedDBFile.Load(nation,dbFilePath.c_str());
        sortedDBFile.Close();

        GTestHelperObject.deleteFile("dbfiles/nation.bin");GTestHelperObject.deleteFile(dbFilePath.c_str());
        string prefloc = GTestHelperObject.dbfile_dir+"nation.pref";GTestHelperObject.deleteFile(prefloc.c_str());    
}
// -------------------------------------------------------------------------------------------------------------
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}