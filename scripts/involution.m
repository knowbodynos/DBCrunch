(* ::Package:: *)

$HistoryLength=0;

WorkingPath/:Set[WorkingPath,_]:=(ClearAll[WorkingPath];WorkingPath=$CommandLine[[5]]);
IntermediateName/:Set[IntermediateName,_]:=(ClearAll[IntermediateName];IntermediateName=$CommandLine[[6]]);

Get["cohomCalgKoszulExtensionSilent`"];
Get["ToricCY`"];


Involutions[FundGp_,ResCWS_,ITensXD_,SRIdeal_,FormatString_:True]:=Module[{TDivs,DivTrivCohom,DivEuler,DivCohom,SameDivCohomSets,SameDivCohomPairs,NaiveInvolInds,DisjointInvolInds,DisjointInvols,SRInvols,i,j,Result},
    TDivs=Table[ToExpression["D"<>ToString[i]],{i,Length[ResCWS]}];
    Quiet[DivTrivCohom=Map[CohomologyOf["Lambda0CotangentBundle",{TDivs,Map[Variables[#]&,SRIdeal],ResCWS},{Plus@@ResCWS,#},"Verbose-1"]&,ResCWS]];
    DivEuler=FundGp*Table[Expand[ITensXD[[i,i,i]]+(1/2)*(Total[ITensXD[[i]],2]-Tr[ITensXD[[i]]])],{i,Length[ITensXD]}];
    DivCohom=MapThread[Join[#1,{#2-(Plus@@(2*#1[[{1,3}]])-(4*#1[[2]]))}]&,{DivTrivCohom,DivEuler}];
    SameDivCohomSets=Select[Gather[Range[Length[TDivs]],DivCohom[[#1]]==DivCohom[[#2]]&],Length[#]>1&];
    SameDivCohomPairs=Select[Join@@Map[Subsets[#,{2}]&,SameDivCohomSets],Unequal@@ResCWS[[#]]&];
    NaiveInvolInds=Subsets[SameDivCohomPairs][[2;;]];
    DisjointInvolInds=Select[NaiveInvolInds,Or@@(IntersectingQ@@@Subsets[#,{2}])==False&];
    DisjointInvols=DisjointInvolInds/.{i_Integer,j_Integer}->Sequence[Rule[Indexed[TDivs,i],Indexed[TDivs,j]],Rule[Indexed[TDivs,j],Indexed[TDivs,i]]];
    SRInvols=Select[DisjointInvols,Intersection[SRIdeal,SRIdeal/.#]==Union[SRIdeal,SRIdeal/.#]&];
    (*SRInvolCohoms=Map[Table[Cohoms[[First[Flatten[Position[TDivs,#[[i,1]]]]]]],{i,1,Length[#],2}]&,SRInvols];*)
    If[FormatString,
        Result={"DIVCOHOM"->ToString[DivCohom,InputForm],"INVOLLIST"->MapIndexed[{"INVOLN"->#2[[1]],"INVOL"->ToString[#1,InputForm]}&,SRInvols]};
    ,
        Result={"DIVCOHOM"->DivCohom,"INVOLLIST"->MapIndexed[{"INVOLN"->#2[[1]],"INVOL"->#1}&,SRInvols]};
    ];
    Return[Result];      
];


MongoDirac=MongoClient[$CommandLine[[7]]];
ToricCYDirac=MongoDB[MongoDirac];
(*TimeLimit=ToExpression[$CommandLine[[8]]];
MemoryLimit=ToExpression[$CommandLine[[9]]];
SkippedFile=$CommandLine[[10]];*)
Geometry=Map[#[[1]]->ToExpression[#[[2]]]&,ToExpression[$CommandLine[[8]]]];

PolyID="POLYID"/.Geometry;
GeomN="GEOMN"/.Geometry;
TriangN="TRIANGN"/.Geometry;
H11="H11"/.Geometry;
FundGp="FUNDGP"/.Geometry;
ResCWS=Transpose["RESCWS"/.Geometry];
ITensXD="ITENSXD"/.Geometry;
SRIdeal="SRIDEAL"/.Geometry;

(*timemem=TimeConstrained[
    MemoryConstrained[
        AbsoluteTiming[MaxMemoryUsed[involutionsresult=Involutions[FundGp,ResCWS,ITensXD,SRIdeal,True]]]
    ,MemoryLimit,"MemorySkipped"]
,TimeLimit,"TimeSkipped"];*)

(*result=TimeConstrained[
    MemoryConstrained[
        Involutions[FundGp,ResCWS,ITensXD,SRIdeal,True]
    ,MemoryLimit,"MemorySkipped"]
,TimeLimit,"TimeSkipped"];

If[!MemberQ[{"TimeSkipped","MemorySkipped"},result],
    (*involutionsstats=timemem;
    {involutionstime,involutionsmem}=involutionsstats;
    result=involutionsresult;*)
    TriangIDField=Thread[{"H11","POLYID","GEOMN","TRIANGN"}->{H11,PolyID,GeomN,TriangN}];
    NewTriangFields={"DIVCOHOM"->("DIVCOHOM"/.result),"NINVOLS"->Length["INVOLLIST"/.result]};
    InvolDoc=Map[Join[TriangIDField,#]&,"INVOLLIST"/.result];
    outresult=Join[NewTriangFields,{"INVOLLIST"->InvolDoc}];
    
    (ToricCYDirac@getCollection["TRIANG"])@update[StringRulestoJSONJava@TriangIDField,StringRulestoJSONJava@{"$set"->NewTriangFields}];
    storage=BSONSize[NewTriangFields];
    If[Length[InvolDoc]==0,InvolDoc={Join[TriangIDField,{"INVOLN"->Null,"INVOL"->Null}]}];
    (ToricCYDirac@getCollection["INVOL"])@insert[StringRulestoJSONJava@InvolDoc];
    storage=storage+BSONSize[InvolDoc];
	output=StringReplace[StringRulestoJSON[outresult],{" "->""}];
    (*WriteString[$Output,"Total Time: "<>ToString[involutionstime]<>"\n"];
    WriteString[$Output,"Total Max Memory: "<>ToString[involutionsmem]<>"\n"];
    WriteString[$Output,"Total Storage: "<>ToString[storage]<>"\n"];*)
,
	(*output=timemem;*)
    output=result;
    WriteString[SkippedFile,ToString[Row[{PolyID,"_",GeomN,"_",TriangN," ",output,"\n"}],InputForm]];
];*)

result=Involutions[FundGp,ResCWS,ITensXD,SRIdeal,True];
TriangIDField=Thread[{"H11","POLYID","GEOMN","TRIANGN"}->{H11,PolyID,GeomN,TriangN}];
NewTriangFields={"DIVCOHOM"->("DIVCOHOM"/.result),"NINVOLS"->Length["INVOLLIST"/.result]};
InvolDoc=Map[Join[TriangIDField,#]&,"INVOLLIST"/.result];
outresult=Join[NewTriangFields,{"INVOLLIST"->InvolDoc}];
    
(ToricCYDirac@getCollection["TRIANG"])@update[StringRulestoJSONJava@TriangIDField,StringRulestoJSONJava@{"$set"->NewTriangFields}];
If[Length[InvolDoc]==0,InvolDoc={Join[TriangIDField,{"INVOLN"->Null,"INVOL"->Null}]}];
(ToricCYDirac@getCollection["INVOL"])@insert[StringRulestoJSONJava@InvolDoc];
output=StringReplace[StringRulestoJSON[outresult],{" "->""}];

WriteString[$Output,"Output: "<>output<>"\n"];
(*DeleteDirectory[WorkingPath<>"/"<>IntermediateName,DeleteContents\[Rule]True];*)
MongoDirac@close[];
Exit[];
