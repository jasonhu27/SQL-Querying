/*execute.c*/

//
// Project: Execution of queries for SimpleSQL, includes all
// necessary helper functions for execute_query to run properly
//
// Jason Hu
// Northwestern University
// CS 211, Winter 2023
//

#include <assert.h>
#include <stdbool.h> // true, false
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

//
// #include any other system <.h> files?
//

#include "analyzer.h"
#include "ast.h"
#include "database.h"
#include "execute.h"
#include "parser.h"
#include "resultset.h"
#include "scanner.h"
#include "tokenqueue.h"
#include "util.h"
//
// #include any other of our ".h" files?
//

//
// implementation of function(s), both private and public
//

// create_result 
// 
// takes in a ResultSet, and based on the number of columns in the
// tablemeta, inserts a column in the same position as the meta-data

void create_result(struct TableMeta *tablemeta, struct ResultSet *rs, struct ColumnMeta *col) {
  //loops through the number of columns in the tablemeta
  for (int i = 0; i < tablemeta->numColumns; i++) {
    resultset_insertColumn(rs, i + 1, tablemeta->name, col[i].name, NO_FUNCTION, col[i].colType);
  }
  // resultset_print(rs);
}

// read_resultset
//
// takes in a line of characters from the dataBuffer, and breaks the dataBuffer
// apart into mini strings by replacing each space with a '\0' and places the
// mini-strings into the appropriate column

void read_resultset(char *dataBuffer, int dataBufferSize, struct ResultSet *rs, int *count) {
  char *cp = dataBuffer;
  // loops through each character in the dataBuffer
  for (int j = 0; j < dataBufferSize; j++) {
    
    // if cp is a space, set it to '\0', also break if it equals . to prevent 
    // memory leaks
    if (*cp == ' ') {
      *cp = '\0';
      cp++;
      if (*cp == '.') {
        break;
      }
    }
    // if cp is " or ', indicates that it is a string, so need to continuously
    // looping until the closing " or ' is found so the string stays as one mini-string
    else if (*cp == '"') {
      while (true) {
        cp++;
        if (*cp == '"') {
          cp++;
          break;
        }
      }
    } 
    else if (*cp == '\'') {
      while (true) {
        cp++;
        if (*cp == '\'') {
          cp++;
          break;
        }
      }
    } 
    else {
      cp++;
    }
  }

  resultset_addRow(rs);
  cp = dataBuffer;
  char *start = cp;
  struct RSColumn *rcol = rs->columns;

  // loops through the number of columns in each row, identifying
  // whether the column is an int, real, or string, and applying the
  // correct function to place the value into the resultset row
  for (int m = 1; m <= rs->numCols; m++) {

    // if the column is an int, call atoi and insert
    if (rcol->coltype == COL_TYPE_INT) {
      int num = atoi(cp);
      resultset_putInt(rs, *count, m, num);
    }

    // if the column is a real, call atof and insert
    if (rcol->coltype == COL_TYPE_REAL) {
      double flo = atof(cp);
      resultset_putReal(rs, *count, m, flo);
    }

    // if the column is a string, advance cp and insert a '\0' at length - 2
    // to remove the quotations, then insert into resultset
    if (rcol->coltype == COL_TYPE_STRING) {
      int num = strlen(cp);
      char temp[num];
      cp++;
      strcpy(temp, cp);
      temp[num - 2] = '\0';
      resultset_putString(rs, *count, m, temp);
    }
    rcol = rcol->next;

    int count = 0;

    // while cp doesn't equal '\0', aka the next mini-string, keep advancing
    while (true) {
      if (*cp == '\0') {
        cp++;
        break;
      }
      cp++;
    }
  }
}

// intRowCheck 
//
// if the col value is of type int, checks through all of the different possible operators
// and applies the correct one, if the value doesn't satisfy the operator, the row is
// deleted

void intRowCheck(struct ResultSet *rs, struct WHERE *where1, int count, int i) {
  if (where1->expr->operator== EXPR_LT) {
    if (!(resultset_getInt(rs, i, count) < atoi(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_LTE) {
    if (!(resultset_getInt(rs, i, count) <= atoi(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_GT) {
    if (!(resultset_getInt(rs, i, count) > atoi(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_GTE) {
    if (!(resultset_getInt(rs, i, count) >= atoi(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_EQUAL) {
    if (!(resultset_getInt(rs, i, count) == atoi(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_NOT_EQUAL) {
    if (!(resultset_getInt(rs, i, count) != atoi(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
}

// realRowCheck 
//
// if the col value is of type real, checks through all of the different possible operators
// and applies the correct one, if the value doesn't satisfy the operator, the row is
// deleted

void realRowCheck(struct ResultSet *rs, struct WHERE *where1, int count, int i) {
  if (where1->expr->operator== EXPR_LT) {
    if (!(resultset_getReal(rs, i, count) < atof(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_LTE) {
    if (!(resultset_getReal(rs, i, count) <= atof(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_GT) {
    if (!(resultset_getReal(rs, i, count) > atof(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_GTE) {
    if (!(resultset_getReal(rs, i, count) >= atof(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_EQUAL) {
    if (!(resultset_getReal(rs, i, count) == atof(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_NOT_EQUAL) {
    if (!(resultset_getReal(rs, i, count) != atof(where1->expr->value))) {
      resultset_deleteRow(rs, i);
    }
  }
}

// stringRowCheck 
//
// if the col value is of type string, checks through all of the different possible operators
// and applies the correct one, if the value doesn't satisfy the operator, the row is
// deleted, freeing the memory at the end

void stringRowCheck(struct ResultSet *rs, struct WHERE *where1, int count, int i) {
  char *cp = resultset_getString(rs, i, count);
  if (where1->expr->operator== EXPR_LT) {
    if (!(strcmp(cp, where1->expr->value) < 0)) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_LTE) {
    if (!(strcmp(cp, where1->expr->value) <= 0)) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_GT) {
    if (!(strcmp(cp, where1->expr->value) > 0)) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_GTE) {
    if (!(strcmp(cp, where1->expr->value) >= 0)) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_EQUAL) {
    if (!(strcmp(cp, where1->expr->value) == 0)) {
      resultset_deleteRow(rs, i);
    }
  }
  if (where1->expr->operator== EXPR_NOT_EQUAL) {
    if (!(strcmp(cp, where1->expr->value) != 0)) {
      resultset_deleteRow(rs, i);
    }
  }
  free(cp);
}

// rowCheck
//
// given a particular row, calls funcitons to determine if the row
// meets the where clause or if it should be deleted

void rowCheck(struct ResultSet *rs, struct RSColumn *rcol, struct WHERE *where1, int count, int i) {
  if (rcol->coltype == COL_TYPE_INT) {
    intRowCheck(rs, where1, count, i);
  }
  if (rcol->coltype == COL_TYPE_REAL) {
    realRowCheck(rs, where1, count, i);
  }
  if (rcol->coltype == COL_TYPE_STRING) {
    stringRowCheck(rs, where1, count, i);
  }
}

// where_shifter
//
// if the where clause is not NULL, finds the correct column that the 
// clause is applying too, and checks whether or not it should be deleted

void where_shifter(struct ResultSet *rs, struct SELECT *select) {
  if (select->where != NULL) {
    struct RSColumn *rcol = rs->columns;
    struct WHERE *where1 = select->where;
    int count = 1;

    // loops through the number of columns, checking if the name matches
    // with the where clause name
    for (int j = 1; j <= rs->numCols; j++) {
      if (strcasecmp(rcol->colName, where1->expr->column->name) == 0) {
        break;
      }
      count++;
      rcol = rcol->next;
    }

    // loops through the rows in reverse order, deleting rows as needed
    for (int i = rs->numRows; i > 0; i--) {
      rowCheck(rs, rcol, where1, count, i);
    }
  }
  // resultset_print(rs);
}

// delete_col
//
// checks whether or not there are columns from the resultset that aren't
// in the query, and if there are, deletes them

void delete_col(struct ResultSet *rs, struct TableMeta *tablemeta, struct Database *db, struct SELECT *select) {
  struct RSColumn *rcol = rs->columns;

  // creates a COLUMN struct temp that points to the start of the columns
  // linked-list, and a COLUMN struct col to traverse through
  struct COLUMN *temp = select->columns;
  struct COLUMN *col = temp;

  int count = 0;

  // for loop that loops through the number of columns in the table
  // for each column, iterates through the COLUMN linked list to see
  // if there are any matches, starting at the end so removed
  // columns don't cause shift issues
  for (int i = tablemeta->numColumns; i >= 1; i--) {

    // while there are still more columns
    while (col != NULL) {
      if (strcasecmp(col->name, tablemeta->columns[i - 1].name) == 0) {
        count = 1;
      }
      col = col->next;
    }
    // if the count is 0, there were no matches, so the column should
    // be deleted
    if (count == 0) {
      resultset_deleteColumn(rs, i);
    }
    // resets col back to the start of the linked-list
    col = temp;
    count = 0;
  }
  // resultset_print(rs);
}

// moveColumn
//
// reorders the columns so that the resultset columns matches what was
// entered in the query

void moveColumn(struct ResultSet *rs, struct SELECT *select) {
  struct COLUMN *col = select->columns;
  int count = 1;

  // while there are still columns in the linked-list
  while (true) {
    if (col == NULL) {
      break;
    }
    // moves the column to its appropriate place
    int res = resultset_findColumn(rs, 1, rs->columns->tableName, col->name);
    resultset_moveColumn(rs, res, count);
    count++;
    col = col->next;
  }
}

// aggFunction
//
// applies the correct function to each column in the resultset

void aggFunction(struct ResultSet *rs, struct SELECT *select) {
  struct COLUMN *col = select->columns;
  int count = 1;

  // loops while there still are more columns
  while (true) {
    if (col == NULL) {
      break;
    }

    // applies the correct function if the column has one
    if (col->function != NO_FUNCTION) {
      resultset_applyFunction(rs, col->function, count);
    }
    count++;
    col = col->next;
  }
}

// appLimit
//
// applies the limit clause by deleting all of the rows past the
// limi value

void appLimit(struct ResultSet *rs, struct SELECT *select) {
  struct LIMIT *lim = select->limit;
  int rest = lim->N;

  // loops through the rows in reverse order
  for (int i = rs->numRows; i >= 0; i--) {

    // if the row number is greater than the limit, delete the row
    if (i > rest) {
      resultset_deleteRow(rs, i);
    }
  }
}

//
// execute_query
//
// execute a select query, with all of the specific functions 
// applied so the output matches what the query is saying
//

void execute_query(struct Database *db, struct QUERY *query) {
  if (db == NULL) {
    panic("db is NULL (execute)");
  }
  if (query == NULL) {
    panic("query is NULL (execute)");
  }
  if (query->queryType != SELECT_QUERY) {
    printf("**INTERNAL ERROR: execute() only supports SELECT queries.\n");
    return;
  }

  struct SELECT *select = query->q.select; // alias for less typing:

  //
  // the query has been analyzed and so we know it's correct: the
  // database exists, the table(s) exist, the column(s) exist, etc.
  //

  //
  // (1) we need a pointer to the table meta data, so find it:
  //
  struct TableMeta *tablemeta = NULL;

  for (int t = 0; t < db->numTables; t++) {
    if (icmpStrings(db->tables[t].name, select->table) == 0) { // found it:
      tablemeta = &db->tables[t];
      break;
    }
  }

  assert(tablemeta != NULL);

  //
  // (2) open the table's data file
  //
  // the table exists within a sub-directory under the executable
  // where the directory has the same name as the database, and with
  // a "TABLE-NAME.data" filename within that sub-directory:
  //
  char path[(2 * DATABASE_MAX_ID_LENGTH) + 10];

  strcpy(path, db->name); // name/name.data
  strcat(path, "/");
  strcat(path, tablemeta->name);
  strcat(path, ".data");

  FILE *datafile = fopen(path, "r");
  if (datafile == NULL) { // unable to open:
    printf("**INTERNAL ERROR: table's data file '%s' not found.\n", path);
    exit(-1);
  }

  //
  // (3) allocate a buffer for input, and start reading the data:
  //
  int dataBufferSize = tablemeta->recordSize + 3; // ends with $\n + null terminator
  char *dataBuffer = (char *)malloc(sizeof(char) * dataBufferSize);
  if (dataBuffer == NULL)
    panic("out of memory");

  // creates the resultset
  struct ColumnMeta *col = tablemeta->columns;
  struct ResultSet *rs = resultset_create();

  create_result(tablemeta, rs, col);

  int count = 1;

  // loops while there are additional lines in the datafile
  while (true) {
    fgets(dataBuffer, dataBufferSize, datafile);
    if (feof(datafile)) { // end of the data file, we're done
      break;
    }

    read_resultset(dataBuffer, dataBufferSize, rs, &count);

    count++;
  }
  // resultset_print(rs);

  // closes the datafile and frees the dataBuffer memory
  fclose(datafile);

  free(dataBuffer);

  // applies all of the helper functions
  where_shifter(rs, select);

  delete_col(rs, tablemeta, db, select);

  moveColumn(rs, select);

  aggFunction(rs, select);

  // calls appLimit if the limit clause isn't NULL
  if (select->limit != NULL) {
    appLimit(rs, select);
  }

  resultset_print(rs);

  resultset_destroy(rs);

  //
  // done!
  //
}