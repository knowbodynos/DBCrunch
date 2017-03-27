(* ::Package:: *)

$HistoryLength=0;

(*WorkingPath/:Set[WorkingPath,_]:=(ClearAll[WorkingPath];WorkingPath=$CommandLine[[6]]);
IntermediateName/:Set[IntermediateName,_]:=(ClearAll[IntermediateName];IntermediateName=$CommandLine[[7]]);*)

(*Get["cohomCalgKoszulExtensionSilent`"];*)
Get["MongoLink`"];
(*Get["ToricCY`"];*)


ExplicitCheck[ITensXJ_,KahlerMat_,FormatString_:True]:=Module[{h11,t,Volfromt,taufromt,tCoeffs,tauCoeffs,explicitCondCoeffs,explicitGB,explicitCond,explicitSols,explicitSol,S,Tc,tC,tauD,ITensXJC,VolfromtC,LVKCCond,SSols,SSol,VolCoeffs,VolCondCoeffs,VolGB,VolCond,VolSols,VolSol,VolfromtauD,VolfromtauDCoeffs},
    (*TimeConstrained[
        MemoryConstrained[*)
    h11=Length[ITensXJ];
    t=Table[ToExpression["t"<>ToString[i]],{i,h11}];
    Volfromt=(1/6)*Sum[t[[i]]*t[[j]]*t[[k]]*ITensXJ[[i,j,k]],{i,h11},{j,h11},{k,h11}];
    taufromt=Map[D[Volfromt,#]&,t];
    tCoeffs=Table[ToExpression["ct"<>ToString[i]<>ToString[j]],{i,h11},{j,h11}];
    tauCoeffs=Table[ToExpression["ctau"<>ToString[i]<>ToString[j]],{i,h11},{j,h11}];
    explicitCondCoeffs=DeleteCases[Flatten[CoefficientList[(tCoeffs.t)^2-(tauCoeffs.taufromt),t]],0];
    explicitGB=GroebnerBasis[explicitCondCoeffs,Variables[{tCoeffs,tauCoeffs}]];
    explicitCond=And@@Thread[explicitGB==0]&&(Det[tCoeffs]!=0)&&(Det[tauCoeffs]!=0);
    explicitSols=FindInstance[explicitCond,Variables[{tCoeffs,tauCoeffs}],Reals];
    If[Length[explicitSols]>0,
        explicitSol=First[explicitSols];
        S=DiagonalMatrix[Table[ToExpression["s"<>ToString[i]],{i,h11}]];
        Tc=S.Transpose[Inverse[tCoeffs/.explicitSol]];
        tC=Table[ToExpression["tc"<>ToString[i]],{i,h11}];
        ITensXJC=Table[Sum[ITensXJ[[i,j,k]]*Transpose[Tc][[i,r]]*Transpose[Tc][[j,s]]*Transpose[Tc][[k,t]],{i,h11},{j,h11},{k,h11}],{r,h11},{s,h11},{t,h11}];
        VolfromtC=(1/6)*Sum[tC[[i]]*tC[[j]]*tC[[k]]*ITensXJC[[i,j,k]],{i,h11},{j,h11},{k,h11}];
        LVKCCond=(VolfromtC>0)&&(And@@Thread[(KahlerMat.Transpose[Tc].tC)>0])&&(And@@Map[(#==-1)||(#==1)&,Variables[S]]);
        SSols=Normal[Quiet[Solve[LVKCCond,Variables[S],Reals]],ConditionalExpression];
        If[Length[SSols]>0,
            SSol=First[SSols];
            VolCoeffs=Table[ToExpression["cVol"<>ToString[i]],{i,h11}];
            VolCondCoeffs=DeleteCases[Flatten[CoefficientList[(VolCoeffs.tC^3)-VolfromtC,tC]],0];
            VolGB=GroebnerBasis[VolCondCoeffs,Variables[{VolCoeffs,S}]];
            VolCond=(And@@Thread[VolGB==0])&&(And@@Map[(#==-1)||(#==1)&,Variables[S]]);
            VolSols=Quiet[Solve[VolCond,Variables[{VolCoeffs,S}],Reals]];
            If[Length[VolSols]>0,
                VolSol=First[VolSols];
                If[FormatString,
                    Return[{"EXPLICIT"->{"RMAT2CYCLE"->ToString[Tc/.VolSol,InputForm],"DIAGCOEFFS"->ToString[VolCoeffs/.VolSol,InputForm]}}];
                ,
                    Return[{"EXPLICIT"->{"RMAT2CYCLE"->Tc/.VolSol,"DIAGCOEFFS"->VolCoeffs/.VolSol}}];
                ];
            ];
            If[FormatString,
                Return[{"EXPLICIT"->{"RMAT2CYCLE"->ToString[Tc/.SSol,InputForm]}}];
            ,
                Return[{"EXPLICIT"->{"RMAT2CYCLE"->Tc/.SSol}}];
            ];
        ];
        Return[{}];
    ];
    Return[{}];
            (*],MemoryLimit,
        ],TimeLimit,
    ]*)
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
H11="H11"/.Geometry;
ITensXJ="ITENSXJ"/.Geometry;
KahlerMat="KAHLERMAT"/.Geometry;

result=ExplicitCheck[ITensXJ,KahlerMat,True];
SwissCheeseIDField=Thread[{"H11","POLYID","GEOMN"}->{H11,PolyID,GeomN}];
If[Length[result]==0,
    WriteString[$Output,"None"];
,
    SwissCheeseDoc=StringRulestoJSON[Join[result,SwissCheeseIDField]];
    WriteString[$Output,"+SWISSCHEESE.{\"POLYID\":"<>ToString[PolyID]<>",\"GEOMN\":"<>ToString[GeomN]<>"}>"<>SwissCheeseDoc];
]
WriteString[$Output,"\n"];

Exit[];
