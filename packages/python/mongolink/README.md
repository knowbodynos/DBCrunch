# mongolink
A package that allows a continuous non-blocking read of large batches of documents from a MongoDB database (remote or local), with some action performed on each batch.

------------------------------------------------------------------------------------------------------------

Installation instructions:

1) Download the `mongolink` package

```
   git clone https://github.com/knowbodynos/mongolink.git
```

2) Navigate into the main directory

```
   cd mongolink
```

3) Install `mongolink`

```
   python setup.py install
```

------------------------------------------------------------------------------------------------------------

Using `mongolink`:

1) Queries are given in the form of a `python` list of lists:

```
   [['<COLLECTION_1>',<JSON_QUERY_1>,<JSON_PROJECTIONS_1>,<OPTIONS_1>], ['<COLLECTION_2>',<JSON_QUERY_2>,<JSON_PROJECTIONS_2>,<OPTIONS_2>], ...]
```

with

   1) `<COLLECTION_#>` is the name of the collection in the database.

   2) `<JSON_QUERY_#>` is a query of the form `{'_id': 10}`.

   3) `<JSON_PROJECTIONS_#>` is a projection of the form `{'_id': 0}`.

   4) `<OPTIONS_#>` is a dictionary of options like HINT,SKIP,SORT,LIMIT,COUNT of the form `{'HINT': {'<FIELD_1>':1}, 'SKIP': 5, 'SORT': {'<FIELD_2>': 1}, 'LIMIT': 10, 'COUNT': True}`.

2) The main function is `dbcrawl`:

```
   dbcrawl(db,queries,statefilepath,statefilename="querystate",inputfunc=lambda x:{"nsteps":1},inputdoc={"nsteps":1},action=printasfunc,readform=lambda x:eval(x),writeform=lambda x:x,timeleft=lambda:1,counters=[1,1],counterupdate=lambda x:None,resetstatefile=False,limit=None,toplevel=True,initdoc={})
```

where

   1) `db` is an `pymongo` database object.

   2) `queries` is a query of the form in step 1.

   3) `statefilepath` is a path to where an intermediate file will be stored, and `statefilename` is its filename.

   4) `inputfunc` is a function that returns a dictionary with information that will be used for reading in documents. `inputdoc` is the first dictionary that is preloaded. `nsteps` refers to the number of documents that will be read in each batch.

   5) `action` is a function that performs an action of each batch of documents.

   6) `readform` and `writeform` allow you to alter the format in which processed documents are stored in the intermediate file `statefilename`.

   7) `timeleft` is a function that returns how much time (in seconds) is left before some limit is reached (default: no limit).

   8) `counters` is a list containing a batch counter and a document counter. They are both initialized at 1 by default.

   9) `resetstatefile` is True or False depending on whether the intermediate file `statefilename` should be overwritten.

   10) `limit` is a limit on how many documents should be processed total. If there is no limit, set to None (default).

   11) `toplevel` and `initdoc` are internal recursive variables and should not be customized.

3) Some useful actions are:

   1) To print batches of file to screen, set `action = printasfunc`

   2) To add batches of documents to a list of batches, set `action = lambda x,y,z: my_list.append(z)`

   3) To add batches of documents to a list of documents, set `action = lambda x,y,z: my_list.extend(z)`

   4) To write batches of documents to a file, set `action = lambda x,y,z: writeasfunc("<FILE_PATH>",z)`