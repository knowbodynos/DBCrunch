(* ::Package:: *)

$HistoryLength=0;

WorkingPath/:Set[WorkingPath,_]:=(ClearAll[WorkingPath];WorkingPath=$CommandLine[[6]]);
IntermediateName/:Set[IntermediateName,_]:=(ClearAll[IntermediateName];IntermediateName=$CommandLine[[7]]);

Get["cohomCalgKoszulExtensionSilent`"];
Get["MongoLink`"];
(*Get["ToricCY`"];*)


DivCohoms[FundGp_,ResCWS_,ITensXD_,SRIdeal_,FormatString_:True]:=Module[{TDivs,InvolDivs,InvolDivInds,DivTrivCohom,DivEuler,DivCohom,Result},
    TDivs=Table[ToExpression["D"<>ToString[i]],{i,Length[ResCWS]}];
    Quiet[DivTrivCohom=Map[CohomologyOf["Lambda0CotangentBundle",{TDivs,Map[Variables[#]&,SRIdeal],ResCWS},{Plus@@ResCWS,#},"Verbose-1"]&,ResCWS]];
    DivEuler=FundGp*Table[Expand[ITensXD[[i,i,i]]+(1/2)*(Total[ITensXD[[i]],2]-Tr[ITensXD[[i]]])],{i,Length[ITensXD]}];
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
FundGp="FUNDGP"/.Geometry;
ResCWS="RESCWS"/.Geometry;
ITensXD="ITENSXD"/.Geometry;
SRIdeal="SRIDEAL"/.Geometry;

result=DivCohoms[FundGp,ResCWS,ITensXD,SRIdeal,True];
NewTriangFields=result;
outNewTriangFields=StringRulestoJSON[NewTriangFields];
WriteString[$Output,"+TRIANG.{\"POLYID\":"<>ToString[PolyID]<>",\"GEOMN\":"<>ToString[GeomN]<>",\"TRIANGN\":"<>ToString[TriangN]<>"}>"<>outNewTriangFields];
WriteString[$Output,"\n"];
Exit[];
