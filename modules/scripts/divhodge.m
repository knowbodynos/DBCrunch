(* ::Package:: *)

$HistoryLength=0;

WorkingPath/:Set[WorkingPath,_]:=(ClearAll[WorkingPath];WorkingPath=$CommandLine[[6]]);
IntermediateName/:Set[IntermediateName,_]:=(ClearAll[IntermediateName];IntermediateName=$CommandLine[[7]]);

Get["cohomCalgKoszulExtensionSilent`"];
Get["MongoLink`"];
(*Get["ToricCY`"];*)


DivCohoms[FundGp_,ResCWS_,ITensXD_,SRIdeal_,Invol_,FormatString_:True]:=Module[{TDivs,InvolDivs,InvolDivInds,DivTrivCohom,DivEuler,DivCohom,Result},
    TDivs=Table[ToExpression["D"<>ToString[i]],{i,Length[ResCWS]}];
    InvolDivs=DeleteDuplicates[Map[Sort[#][[1]]&,Invol/.Rule->List]];
    InvolDivInds=Map[Position[TDivs,#][[1,1]]&,InvolDivs];
    Quiet[DivTrivCohom=Map[CohomologyOf["Lambda0CotangentBundle",{TDivs,Map[Variables[#]&,SRIdeal],ResCWS},{Plus@@ResCWS,#},"Verbose-1"]&,ResCWS[[InvolDivInds]]]];
    DivEuler=FundGp*Map[Expand[ITensXD[[#,#,#]]+(1/2)*(Total[ITensXD[[#]],2]-Tr[ITensXD[[#]]])]&,InvolDivInds];
    DivCohom=MapThread[Join[#1,{#2-(Plus@@(2*#1[[{1,3}]])-(4*#1[[2]]))}]&,{DivTrivCohom,DivEuler}];
    If[FormatString,
        Result={"DIVCOHOM"->Map[ToString[#,InputForm]&,DivCohom]};
    ,
        Result={"DIVCOHOM"->DivCohom};
    ];
    Return[Result];
];


Geometry=JSONtoExpressionRules[$CommandLine[[5]]];

PolyID="POLYID"/.Geometry;
GeomN="GEOMN"/.Geometry;
TriangN="TRIANGN"/.Geometry;
InvolN="INVOLN"/.Geometry;
FundGp="FUNDGP"/.Geometry;
ResCWS="RESCWS"/.Geometry;
ITensXD="ITENSXD"/.Geometry;
SRIdeal="SRIDEAL"/.Geometry;
Invol="INVOL"/.Geometry;

result=DivCohoms[FundGp,ResCWS,ITensXD,SRIdeal,Invol,True];
TriangIDField=Thread[{"H11","POLYID","GEOMN","TRIANGN"}->{H11,PolyID,GeomN,TriangN}];
NewTriangFields=result;
outNewTriangFields=StringRulestoJSON[NewTriangFields];
WriteString[$Output,"+INVOL.{\"POLYID\":"<>ToString[PolyID]<>",\"GEOMN\":"<>ToString[GeomN]<>",\"TRIANGN\":"<>ToString[TriangN]<>",\"INVOLN\":"<>ToString[InvolN]<>"}>"<>outNewTriangFields];
WriteString[$Output,"\n"];
Exit[];
