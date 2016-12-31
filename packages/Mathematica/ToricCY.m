(* ::Package:: *)

Needs["JLink`"];
(*RemoteDBUsername="manager";
RemoteDBPassword="toric";
RemoteHostDirac="129.10.135.170";
RemotePortDirac="27017";
RemoteHostWebfaction="45.79.10.158";
RemotePortWebfaction="28555";
RemoteDB="ToricCY";*)
MongoDBJarDirectory=DirectoryName[$InputFileName];
SetOptions[ReinstallJava, JVMArguments->"-Xmx32g"];
ReinstallJava[];
AddToClassPath[MongoDBJarDirectory,"Prepend"->True];
LoadJavaClass@"com.mongodb.util.JSON";
(*mongoDirac=JavaNew["com.mongodb.MongoClient",JavaNew["com.mongodb.MongoClientURI","mongodb://"<>RemoteDBUsername<>":"<>RemoteDBPassword<>"@"<>RemoteHostDirac<>":"<>RemotePortDirac<>"/"<>RemoteDB]];
mongoWebfaction=JavaNew["com.mongodb.MongoClient",JavaNew["com.mongodb.MongoClientURI","mongodb://"<>RemoteDBUsername<>":"<>RemoteDBPassword<>"@"<>RemoteHostWebfaction<>":"<>RemotePortWebfaction<>"/"<>RemoteDB]];
ToricCYDirac=mongoDirac@getDB["ToricCY"];
POLYDirac=ToricCYDirac@getCollection["POLY"];
GEOMDirac=ToricCYDirac@getCollection["GEOM"];
TRIANGDirac=ToricCYDirac@getCollection["TRIANG"];
INVOLDirac=ToricCYDirac@getCollection["INVOL"];
ToricCYWebfaction=mongoWebfaction@getDB["ToricCY"];
POLYWebfaction=ToricCYWebfaction@getCollection["POLY"];
GEOMWebfaction=ToricCYWebfaction@getCollection["GEOM"];
TRIANGWebfaction=ToricCYWebfaction@getCollection["TRIANG"];
INVOLWebfaction=ToricCYWebfaction@getCollection["INVOL"];*)

MongoClient[mongouri_]:=JavaNew["com.mongodb.MongoClient",JavaNew["com.mongodb.MongoClientURI",mongouri]];
MongoDB[mongoclient_]:=mongoclient@getDB["ToricCY"];

(*ToricCYTiers=ReadList[FileNameDrop[DirectoryName[$InputFileName],-1]<>"/state/tiers",String];
ToricCYIndexes=ReadList[FileNameDrop[DirectoryName[$InputFileName],-1]<>"/state/indexes",String];*)

(*Get size of document in bytes.*)
BSONSize[doc_]:=(JavaNew["org.bson.BasicBSONEncoder"]@encode[StringRulestoJSONJava@doc])[[1]];

(*Convert JSON document to compressed Mathematica rules document.*)
JSONtoStringRules[doc_]:=ImportString[doc,"JSON"];

(*Convert JSON document to JLink JSON document.*)
JSONtoJSONJava[doc_]:=JSON`parse[doc];

(*Convert compressed Mathematica rules document to JSON document.*)
StringRulestoJSON[doc_]:=ExportString[doc,"JSON","Compact"->True];

(*Convert compressed Mathematica rules document to JLink JSON document.*)
StringRulestoJSONJava[doc_]:=JSONtoJSONJava[StringRulestoJSON[doc]];

(*Convert expanded Mathematica rules document to compressed Mathematica rules document.*)
ExpressionRulestoStringRules[doc_]:=Module[{PosExpressionsToStrings,StringsResult},
    (*PosExpressionsToStrings=Select[Map[Most[#]&,Position[doc,Rule]],((Head[doc[[Sequence@@#]][[1]]]===String)&&(Head[doc[[Sequence@@#]][[2]]]=!=Integer))&];*)
    PosExpressionsToStrings=Position[doc,_?((Head[#]===Rule)&&(Head[#[[1]]]===String)&&(Head[#[[2]]]=!=Integer)&)];
    StringsResult=MapAt[Rule[#[[1]],ToString[#[[2]],InputForm]]&,doc,PosExpressionsToStrings];
    Return[StringsResult];
];

(*Convert expanded Mathematica rules document to JSON document.*)
ExpressionRulestoJSON[doc_]:=StringRulestoJSON[ExpressionRulestoStringRules[doc]];

(*Convert expanded Mathematica rules document to JLink JSON document.*)
ExpressionRulestoJSONJava[doc_]:=JSONtoJSONJava[ExpressionRulestoJSON[doc]];

(*Convert compressed Mathematica rules document to expanded Mathematica rules document.*)
StringRulestoExpressionRules[doc_]:=Module[{PosStringsToExpressions,ExpressionsResult},
    PosStringsToExpressions=Position[doc,_?((Head[#]===Rule)&&(Quiet[Check[ToExpression[#[[2]]],False]]=!=False)&)];
    ExpressionsResult=MapAt[Rule[#[[1]],ToExpression[#[[2]]]]&,doc,PosStringsToExpressions];
    Return[ExpressionsResult];
];

(*Convert JSON document to expanded Mathematica rules document.*)
JSONtoExpressionRules[doc_]:=StringRulestoExpressionRules[JSONtoStringRules[doc]];

(*Query specific collection in database.*)
CollectionFind[DB_,Collection_,Query_,Projection_,FormatResult_:"Expression"]:=Module[{JSONResult,Result,StringsToExpressions,ExpressionResult},
    If[Projection==="Count",
        Result=(DB@getCollection[Collection])@find[StringRulestoJSONJava@Query];
        Return[Result@count[]];
    ,
        JSONResult=(DB@getCollection[Collection])@find[StringRulestoJSONJava@Query,StringRulestoJSONJava@Projection]@toArray[]@toString[];
        (*If[FormatResult==="JSON",
            Result=JSONResult;
        ,
            If[FormatResult==="String",
                Result=JSONtoStringRules[JSONResult];
            ,
                If[FormatResult==="Expression",
                    Result=JSONtoExpressionRules[JSONResult];
                ,
                    Result=None;
                ];
            ];
        ];*)
        Switch[FormatResult,
            "JSON",
                Result=JSONResult,
            "String",
                Result=JSONtoStringRules[JSONResult],
            "Expression",
                Result=JSONtoExpressionRules[JSONResult],
            _,
                Result=None
        ];
        (*Return[Map[Map[If[#[[1]]==="_id",#,#[[1]]->ToExpression[#[[2]]]]&,#]&,Result]];*)
        (*StringsToExpressions=Position[Result,_?((Head[#]===Rule)&&(Quiet[Check[ToExpression[#[[2]]],False]]=!=False)&)];
        ExpressionResult=MapAt[Rule[#[[1]],ToExpression[#[[2]]]]&,Result,StringsToExpressions];
        Return[ExpressionResult];*)
        Return[Result];
    ];
];

GetTiers[DB_]:="TIER"/.CollectionFind[DB ,"TIERS",{},{"_id"->0,"TIER"->1}];

GetIndexes[DB_,Collection_:"All"]:=Module[{Indexes},
    If[Collection=="All",
        Indexes=DeleteDuplicates[Flatten["INDEX"/.("INDEXES"/.CollectionFind[DB ,"TIERS",{},{"_id"->0,"INDEXES.INDEX"->1}])]];
    ,
        Indexes=Flatten["INDEX"/.("INDEXES"/.CollectionFind[DB ,"TIERS",{"TIER"->Collection},{"_id"->0,"INDEXES.INDEX"->1}])];
    ];
    Return[Indexes];
];

(*Check if specific field exists in the collection.*)
(*CollectionFieldExists[DB_,Collection_,Field_]:=(DB@getCollection[Collection])@find[StringRulestoJSONJava@{Field->{"$exists"->True,"$ne"->Null}}]@limit[1]@length[]==1;*)
CollectionFieldExists[DB_,Collection_,Field_]:=!(DB@getCollection[Collection])@find[StringRulestoJSONJava@{},StringRulestoJSONJava@{"_id"->0,Field->1}]@limit[1]@next[]@isEmpty[];

(*List the minimal sets of indexes from one collection's query required to specify documents in the next collection's query.*)
ListIndexes[DB_,Collection_,Filter_,Indexes_]:=Module[{TrueIndexes,TransposeIndexList,IndexList},
    TrueIndexes=Select[Indexes,CollectionFieldExists[DB,Collection,#]&];
    If[Length[TrueIndexes]==0,
        Return[{}];
    ];
    TransposeIndexList=Select[Transpose[Map[Thread[TrueIndexes->#]&,TrueIndexes/.Filter]],Length[Intersection[Map[#[[2]]&,#],TrueIndexes]]==0&];
    IndexList=DeleteDuplicates[If[Length[TransposeIndexList]>0,Transpose[TransposeIndexList],TransposeIndexList]];
    Return[IndexList];
];

(*Check whether documents from two different collection's queries share the same minimal indexes and should be concatenated.*)
SameIndexes[Filter1_,Filter2_,Indexes_]:=Module[{Result},
    Result=Equal@@Transpose[Select[Transpose[Indexes/.{Filter1,Filter2}],Length[Intersection[#,Indexes]]==0&]];
    Return[Result];
];

(*Query all collections in the database and concatenate the documents of each that refer to the same object.*)
QueryDatabase[DB_,Queries_,FormatResult_:"Expression"]:=Module[{TierList,Tiers,Indexes,SortedProjQueries,MaxCountQuery,SortedQueries,TotalResult,IndexList,OrGroup,NextResult,i},
    TierList=CollectionFind[DB ,"TIERS",{},{"_id"->0,"TIER"->1,"INDEXES.INDEX"->1}];    
    Tiers="TIER"/.TierList;
    Indexes=DeleteDuplicates[Flatten["INDEX"/.("INDEXES"/.TierList)]];
    SortedProjQueries=Reverse[SortBy[Select[Queries,#[[3]]=!="Count"&],Join[{Length[#[[2]]]},Flatten[Position[Tiers,#[[1]]],1]]&]];
    MaxCountQuery=MaximalBy[Complement[Queries,SortedProjQueries],Length[#[[2]]]&];
    SortedQueries=Join[SortedProjQueries,MaxCountQuery];
    TotalResult=CollectionFind[DB,Sequence@@SortedQueries[[1]],FormatResult];
    If[SortedQueries[[1,3]]==="Count",
        Return[TotalResult];
    ];
    For[i=2,i<=Length[SortedQueries],i++,
        IndexList=ListIndexes[DB,SortedQueries[[i,1]],TotalResult,Indexes];
        If[Length[IndexList]==0,
            OrGroup=SortedQueries[[i,2]];
        ,
            OrGroup=Join[SortedQueries[[i,2]],{"$or"->IndexList}];
        ];
        NextResult=CollectionFind[DB,SortedQueries[[i,1]],OrGroup,SortedQueries[[i,3]],FormatResult];
        If[SortedQueries[[i,3]]==="Count",
            Return[NextResult];
        ];
        TotalResult=Flatten[Map[Function[u,Map[Union[u,#]&,Select[NextResult,SameIndexes[#,u,Indexes]&]]][#]&,TotalResult],1];
    ];
    Return[TotalResult];
];

(*
Queries={{"POLY",{},{"_id"\[Rule]0,"POLYID"\[Rule]1,"H11"\[Rule]1}},{"GEOM",{"CHERN2XNUMS"\[Rule]"{16,44,26,34}"},{"_id"\[Rule]0,"POLYID"\[Rule]1,"GEOMN"\[Rule]1,"CHERN2XNUMS"\[Rule]1}},{"TRIANG",{},"Count"}};
QueryDatabase[Queries]
*)
