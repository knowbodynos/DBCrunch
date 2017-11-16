(* ::Package:: *)

$HistoryLength=0;

(*WorkingPath/:Set[WorkingPath,_]:=(ClearAll[WorkingPath];WorkingPath=$CommandLine[[6]]);
IntermediateName/:Set[IntermediateName,_]:=(ClearAll[IntermediateName];IntermediateName=$CommandLine[[7]]);

Get["cohomCalgKoszulExtensionSilent`"];*)
Get["MongoLink`"];
(*Get["ToricCY`"];*)


(*Involutions[FundGp_,ResCWS_,ITensXD_,SRIdeal_,FormatString_:True]:=Module[{TDivs,DivTrivCohom,DivEuler,DivCohom,SameDivCohomSets,SameDivCohomPairs,NaiveInvolInds,DisjointInvolInds,DisjointInvols,SRInvols,i,j,Result},*)
Involutions[FundGp_,ResCWS_,IPolyXD_,SRIdeal_,DivCohom_,FormatString_:True]:=Module[{TDivs,SameDivCohomSets,SameDivCohomPairs,NaiveInvolInds,DisjointInvolInds,DisjointInvols,IPolyInvols,i,j,Result},
    TDivs=Table[ToExpression["D"<>ToString[i]],{i,Length[ResCWS]}];
    (*Quiet[DivTrivCohom=Map[CohomologyOf["Lambda0CotangentBundle",{TDivs,Map[Variables[#]&,SRIdeal],ResCWS},{Plus@@ResCWS,#},"Verbose-1"]&,ResCWS]];
    DivEuler=FundGp*Table[Expand[ITensXD[[i,i,i]]+(1/2)*(Total[ITensXD[[i]],2]-Tr[ITensXD[[i]]])],{i,Length[ITensXD]}];
    DivCohom=MapThread[Join[#1,{#2-(Plus@@(2*#1[[{1,3}]])-(4*#1[[2]]))}]&,{DivTrivCohom,DivEuler}];*)
    SameDivCohomSets=Select[Gather[Range[Length[TDivs]],DivCohom[[#1]]==DivCohom[[#2]]&],Length[#]>1&];
    SameDivCohomPairs=Select[Join@@Map[Subsets[#,{2}]&,SameDivCohomSets],Unequal@@ResCWS[[#]]&];
    NaiveInvolInds=Subsets[SameDivCohomPairs][[2;;]];
    DisjointInvolInds=Select[NaiveInvolInds,Or@@(IntersectingQ@@@Subsets[#,{2}])==False&];
    DisjointInvols=DisjointInvolInds/.{i_Integer,j_Integer}->Sequence[Rule[Indexed[TDivs,i],Indexed[TDivs,j]],Rule[Indexed[TDivs,j],Indexed[TDivs,i]]];
    (*ITensInvols={};
    For[i=1,i<=Length[DisjointInvols],i++,
        RMat=Map[Function[u,Map[Coefficient[u,#]&,TDivs]][#]&,TDivs/.DisjointInvols[[i]]];
        RITensXD=Table[Sum[RMat[[r,i]]*RMat[[s,j]]*RMat[[t,k]]*ITensXD[[i,j,k]],{i,Length[TDivs]},{j,Length[TDivs]},{k,Length[TDivs]}],{r,Length[TDivs]},{s,Length[TDivs]},{t,Length[TDivs]}];
        InvarQ=And@@Thread[Flatten[ITensXD-RITensXD]==0];
        If[InvarQ,
            ITensInvols=Join[ITensInvols,{DisjointInvols[[i]]}];
        ];
    ];*)
    IPolyInvols=Select[DisjointInvols,IPolyXD-(IPolyXD/.#)==0&];
    (*SRInvols=Select[DisjointInvols,Intersection[SRIdeal,SRIdeal/.#]==Union[SRIdeal,SRIdeal/.#]&];*)
    (*SRInvolCohoms=Map[Table[Cohoms[[First[Flatten[Position[TDivs,#[[i,1]]]]]]],{i,1,Length[#],2}]&,SRInvols];*)
    If[FormatString,
        Result={"DIVCOHOM"->Map[ToString[#,InputForm]&,DivCohom],"INVOLLIST"->MapIndexed[{"INVOLN"->#2[[1]],"INVOL"->ToString[#1,InputForm],"INVOLDIVCOHOM"->Map[ToString[DivCohom[[Position[TDivs,#][[1,1]]]],InputForm]&,(#1/.Rule[x_,y_]->x)][[Range[1,Length[#1],2]]]}&,IPolyInvols]};
    ,
        Result={"DIVCOHOM"->DivCohom,"INVOLLIST"->MapIndexed[{"INVOLN"->#2[[1]],"INVOL"->#1,"INVOLDIVCOHOM"->Map[DivCohom[[Position[TDivs,#][[1,1]]]]&,(#1/.Rule[x_,y_]->x)][[Range[1,Length[#1],2]]]}&,IPolyInvols]};
    ];
    Return[Result];      
];


(*MongoDirac=MongoClient[$CommandLine[[7]]];
ToricCYDirac=MongoDirac@getDB["ToricCY"];*)
(*TimeLimit=ToExpression[$CommandLine[[8]]];
MemoryLimit=ToExpression[$CommandLine[[9]]];
SkippedFile=$CommandLine[[10]];*)
(*Geometry=Map[#[[1]]->ToExpression[#[[2]]]&,ToExpression[$CommandLine[[7]]]];*)
Geometry=JSONtoExpressionRules[$CommandLine[[5]]];

PolyID="POLYID"/.Geometry;
GeomN="GEOMN"/.Geometry;
TriangN="TRIANGN"/.Geometry;
H11="H11"/.Geometry;
FundGp="FUNDGP"/.Geometry;
ResCWS="RESCWS"/.Geometry;
IPolyXD="IPOLYXD"/.Geometry;
SRIdeal="SRIDEAL"/.Geometry;
DivCohom="DIVCOHOM"/.Geometry;

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
(*result=Involutions[FundGp,ResCWS,ITensXD,SRIdeal,True];*)
result=Involutions[FundGp,ResCWS,IPolyXD,SRIdeal,DivCohom,True];
TriangIDField=Thread[{"H11","POLYID","GEOMN","TRIANGN"}->{H11,PolyID,GeomN,TriangN}];
NewTriangFields={"DIVCOHOM1"->("DIVCOHOM"/.result),"NINVOLS"->Length["INVOLLIST"/.result]};
InvolDocs=Map[Join[TriangIDField,#]&,"INVOLLIST"/.result];
(*If[Length[InvolDoc]==0,InvolDoc={Join[TriangIDField,{"INVOLN"->Null,"INVOL"->Null}]}];*)
(*outresult=Join[{NewTriangFields},InvolDoc];*)
    
(*(ToricCYDirac@getCollection["TRIANG"])@update[StringRulestoJSONJava@TriangIDField,StringRulestoJSONJava@{"$set"->NewTriangFields}];
(ToricCYDirac@getCollection["INVOL"])@insert[StringRulestoJSONJava@InvolDoc];*)
outNewTriangFields=StringRulestoJSON[NewTriangFields];
outInvolDocs=Map[StringRulestoJSON[#]&,InvolDocs];
WriteString[$Output,"+TRIANG.{\"POLYID\":"<>ToString[PolyID]<>",\"GEOMN\":"<>ToString[GeomN]<>",\"TRIANGN\":"<>ToString[TriangN]<>"}>"<>outNewTriangFields];
Do[
    WriteString[$Output,"\n+INVOL.{\"POLYID\":"<>ToString[PolyID]<>",\"GEOMN\":"<>ToString[GeomN]<>",\"TRIANGN\":"<>ToString[TriangN]<>",\"INVOLN\":"<>ToString["INVOLN"/.InvolDocs[[i]]]<>"}>"<>outInvolDocs[[i]]];
,{i,Length[outInvolDocs]}];
WriteString[$Output,"\n"];
(*WriteString[$Output,output<>"\n"];*)
(*DeleteDirectory[WorkingPath<>"/"<>IntermediateName,DeleteContents\[Rule]True];*)
(*MongoDirac@close[];*)
Exit[];
