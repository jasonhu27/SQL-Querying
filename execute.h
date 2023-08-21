/*execute.h*/

//
// Project: Execution of queries for SimpleSQL
//
// Jason Hu 
// Northwestern University
// CS 211, Winter 2023
//

#pragma once

//
// #include header files needed to compile function declarations
//
#include <stdbool.h> // true, false
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "execute.h"
#include "database.h"
#include "ast.h"
#include "tokenqueue.h"
#include "scanner.h"
#include "parser.h"
#include "util.h"
#include "analyzer.h"
//
// function declarations:
//

void execute_query(struct Database* db, struct QUERY* query);
