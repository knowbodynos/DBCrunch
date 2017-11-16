(* ::Package:: *)

$HistoryLength=0;

(*WorkingPath/:Set[WorkingPath,_]:=(ClearAll[WorkingPath];WorkingPath=$CommandLine[[6]]);
IntermediateName/:Set[IntermediateName,_]:=(ClearAll[IntermediateName];IntermediateName=$CommandLine[[7]]);*)

(*Get["cohomCalgKoszulExtensionSilent`"];*)
Get["MongoLink`"];
(*Get["ToricCY`"];*)


dPns[DDivs_,ITensXD_,GenDiv_]:=Module[{s,chernsX,chernsD,TdD,ITensXDRules,DegD,EulerD,HEulerD,c1D,c2D,c3D,c1X,c2X,c3X},
    chernsX=Quiet[Solve[And@@Thread[CoefficientList[(Times@@Map[1+s*#&,DDivs])-(1+s*c1X+s^2*c2X+s^3*c3X)*(1+s*(Plus@@DDivs)),s][[2;;4]]==0],{c1X,c2X,c3X}]][[1]];
    chernsD=Quiet[Solve[And@@Thread[CoefficientList[(Times@@Map[1+s*#&,DDivs])-(1+s*c1D+s^2*c2D+s^3*c3D)*(1+s*(Plus@@DDivs))*(1+s*GenDiv),s][[2;;4]]==0],{c1D,c2D,c3D}]][[1]];
    (*TdX=Collect[Simplify[(1+s*(1/2)*c1X+(1/12)*s^2*(c1X^2+c2X)+(1/24)*s^3*c1X*c2X)/.chernsX],s];
    chernchD=Collect[Simplify[(1+s*c1D+(1/2)*s^2*c1D^2+(1/6)*s^3*c1D^3)/.chernsD],s];*)
    TdD=Collect[Simplify[(1+s*(1/2)*c1D+(1/12)*s^2*(c1D^2+c2D)+(1/24)*s^3*c1D*c2D)/.chernsD],s];
    ITensXDRules=DeleteDuplicates[Flatten[Table[DDivs[[i]]*DDivs[[j]]*DDivs[[k]]->ITensXD[[i,j,k]],{i,Length[DDivs]},{j,Length[DDivs]},{k,Length[DDivs]}]]];
    DegD=FullSimplify[Expand[GenDiv*c1D^2/.chernsD]/.ITensXDRules];
    EulerD=FullSimplify[Expand[GenDiv*c2D/.chernsD]/.ITensXDRules];
    (*HEulerD=FullSimplify[Expand[(Plus@@TDivs)*Coefficient[Simplify[TdX*chernchD],s^2]]/.ITensXDRules];*)
    HEulerD=FullSimplify[Expand[GenDiv*Coefficient[TdD,s^2]]/.ITensXDRules];
    If[9-DegD==EulerD-3,9-DegD,If[(EulerD==24)&&(HEulerD==2),-3,If[(EulerD==0)&&(HEulerD==0),-4,-1]]]
];


ExplicitCheck[ITensXJ_,Basis_,InvBasis_,KahlerMat_,FormatString_:True]:=Module[{JtoDPos,ITensXJRules,ITensXD,h11,K,DDivs,JDivs,PerfSquare\[Tau]Coeffs,t,\[Tau],JtoDDivs,Volt,\[Tau]fromt,KahlerMatt,tMonoms,\[Tau]fromtMonomsMat,PerfSquare\[Tau]CoeffsSols,sols,tfrom\[Tau]Rules,t\[Tau]Sols,PerfectSquares,p,PartialInverses,PartialInverse,ComplementInverses,ComplementInverse,FullInverse,tfrom\[Tau],\[Tau]Pos,\[Tau]PosGroups,\[Tau]BFillPos,\[Tau]BFillfrom\[Tau],h11Diag,\[Tau]B,\[Tau]Bfrom\[Tau],TB,\[Tau]BFillfrom\[Tau]B,tfrom\[Tau]B,Vol\[Tau]B,\[Tau]BCond,\[Tau]BSigns,dPSol,\[Tau]BPermuteRules,tfrom\[Tau]BTerms,Vol\[Tau]BTerms,NDiagTerms,NL},
    h11=Length[Basis];
    K=Length[InvBasis];
    
    DDivs=Table[ToExpression["D"<>ToString[i]],{i,K}];    
    JDivs=Table[ToExpression["J"<>ToString[i]],{i,h11}];
    PerfSquare\[Tau]Coeffs=Table[ToExpression["a"<>ToString[i]],{i,h11}];
    t=Table[ToExpression["t"<>ToString[i]],{i,h11}];
    \[Tau]=Table[ToExpression["\[Tau]"<>ToString[i]],{i,h11}];

    JtoDDivs=Map[#[[1]]->#[[2]]&,Basis];
    
    (*JtoDPos=Flatten[Map[Position[DDivs,#[[2]]]&,Basis]];
    ITensXJ=Table[ITensXD[[i,j,k]],{i,JtoDPos},{j,JtoDPos},{k,JtoDPos}];*)
    
    ITensXJRules=DeleteDuplicates[Flatten[Table[JDivs[[i]]*JDivs[[j]]*JDivs[[k]]->ITensXJ[[i,j,k]],{i,h11},{j,h11},{k,h11}]]];
    ITensXD=Expand[Table[InvBasis[[i,1]]*InvBasis[[j,1]]*InvBasis[[k,1]],{i,K},{j,K},{k,K}]]/.ITensXJRules;

    Volt=Simplify[(1/6)*Sum[ITensXJ[[i,j,k]]*t[[i]]*t[[j]]*t[[k]],{i,h11},{j,h11},{k,h11}]];
    \[Tau]fromt=Simplify[(1/2)*Table[Sum[ITensXJ[[i,j,k]]*t[[j]]*t[[k]],{j,h11},{k,h11}],{i,h11}]];

    KahlerMatt=And@@Thread[(KahlerMat.t)>0];

    tMonoms=Map[#/(#/.Thread[Variables[#]->1])&,MonomialList[Plus@@\[Tau]fromt]];
    \[Tau]fromtMonomsMat=Map[Coefficient[#,tMonoms]&,Expand[\[Tau]fromt]];

    PerfSquare\[Tau]CoeffsSols=Map[
             Function[u,
                 sols=Normal[Quiet[Solve[Thread[GroebnerBasis[(Transpose[\[Tau]fromtMonomsMat].PerfSquare\[Tau]Coeffs)[[Select[Range[Length[tMonoms]],!MemberQ[u,tMonoms[[#]]]&]]],PerfSquare\[Tau]Coeffs]==0],PerfSquare\[Tau]Coeffs,Rationals]],ConditionalExpression];
                 If[Length[sols]>0,
                     sols[[1]]
                 ,
                     sols
                 ]][#]&
         ,Select[DeleteDuplicates[Map[Map[#/(#/.Thread[Variables[#]->1])&,MonomialList[#]]&,DeleteDuplicates[Plus@@@Tuples[t,h11]]^2]],And@@Map[MemberQ[tMonoms,#]&,#]&]];

    If[Length[PerfSquare\[Tau]CoeffsSols]>0,
        t\[Tau]Sols=DeleteCases[Simplify[{PerfSquare\[Tau]Coeffs.\[Tau]fromtMonomsMat.tMonoms,PerfSquare\[Tau]Coeffs.\[Tau]}/.PerfSquare\[Tau]CoeffsSols],{0,0}];
        PerfectSquares=Map[Simplify[Sqrt[#[[1]]],KahlerMatt]==Sqrt[#[[2]]]&,Select[t\[Tau]Sols,(Length[(p=Position[Factor[#[[1]]],Power[_,2]])]==1)&&(Length[p[[1]]]==1)&&(Factor[#[[1]]][[0]]==Times)&]];
        If[Length[PerfectSquares]>0,
            PartialInverses=Quiet[Simplify[Solve[PerfectSquares,t]]];
            If[Length[PartialInverses]>0,
                PartialInverse=PartialInverses[[1]];
                ComplementInverses=Quiet[Solve[\[Tau]==(\[Tau]fromt/.PartialInverse),t]];
                If[Length[ComplementInverses]>0,
                    ComplementInverse=ComplementInverses[[1]];
                    FullInverse=Simplify[Join[ComplementInverse,PartialInverse/.ComplementInverse]];
                    tfrom\[Tau]Rules=FullInverse;
                ,
                    tfrom\[Tau]Rules=PartialInverse;
                ];
                
                tfrom\[Tau]=t/.tfrom\[Tau]Rules;

                \[Tau]Pos=Position[tfrom\[Tau],_?(Function[u,(Or@@Map[u==#&,\[Tau]])||((u[[0]]===Plus)&&(And@@Table[((u[[i,0]]===Times)&&(Or@@Map[u[[i,-1]]==#&,\[Tau]]))||(Or@@Map[u[[i]]==#&,\[Tau]]),{i,Length[u]}]))][#]&)];
                \[Tau]PosGroups=GatherBy[DeleteDuplicates[SortBy[\[Tau]Pos,Length],#2[[;;Length[#1]]]==#1&],tfrom\[Tau][[Sequence@@#]]&];
                {\[Tau]BFillPos,\[Tau]BFillfrom\[Tau]}=Transpose[Reverse[SortBy[Map[{#,tfrom\[Tau][[Sequence@@#[[1]]]]}&,\[Tau]PosGroups],{Length[#[[1]]],-Length[#[[2]]]}&]]];
                h11Diag=Min[Length[\[Tau]BFillfrom\[Tau]],h11];
                \[Tau]B=Table[ToExpression["\[Tau]B"<>ToString[i]],{i,h11Diag}];

                \[Tau]Bfrom\[Tau]=\[Tau]BFillfrom\[Tau][[;;h11Diag]];
                TB=Map[Function[u,Map[Coefficient[u,#]&,\[Tau]]][#]&,\[Tau]Bfrom\[Tau]];
                \[Tau]BFillfrom\[Tau]B=Join[\[Tau]B,Simplify[\[Tau]BFillfrom\[Tau][[h11Diag+1;;]]/.Thread[\[Tau]->(Inverse[TB].\[Tau]B)]]];
                tfrom\[Tau]B=Simplify[ReplacePart[tfrom\[Tau],Join@@Map[Thread[#]&,Thread[\[Tau]BFillPos->\[Tau]BFillfrom\[Tau]B]]],Thread[PerfSquare\[Tau]Coeffs>=0]];
                Vol\[Tau]B=Simplify[Volt/.Thread[t->tfrom\[Tau]B]];
                \[Tau]BCond=Simplify[Reduce[(Join[\[Tau]B,tfrom\[Tau]B,{Vol\[Tau]B}]\[Element]Reals),\[Tau]B,Complexes],Join[t,\[Tau]B]\[Element]Reals];
                \[Tau]BSigns=Map[(-1)^Boole[#===True]&,Simplify[Thread[\[Tau]B<=0],\[Tau]BCond]];
                
                \[Tau]BCond=Simplify[Reduce[(Join[\[Tau]B,tfrom\[Tau]B,{Vol\[Tau]B}]\[Element]Reals)/.Thread[\[Tau]B->\[Tau]BSigns*\[Tau]B],\[Tau]B,Complexes],Join[t,\[Tau]B]\[Element]Reals];
                \[Tau]Bfrom\[Tau]=\[Tau]BSigns*\[Tau]Bfrom\[Tau];
                tfrom\[Tau]B=Simplify[tfrom\[Tau]B/.Thread[\[Tau]B->\[Tau]BSigns*\[Tau]B],\[Tau]BCond];
                Vol\[Tau]B=Simplify[Vol\[Tau]B/.Thread[\[Tau]B->\[Tau]BSigns*\[Tau]B],\[Tau]BCond];
                
                dPSol=Map[dPns[DDivs,ITensXD,#]&,(\[Tau]Bfrom\[Tau]/.Thread[\[Tau]->JDivs])/.JtoDDivs];
                \[Tau]BPermuteRules=Thread[\[Tau]B->\[Tau]B[[Ordering[dPSol]]]];
                \[Tau]BCond=\[Tau]BCond/.\[Tau]BPermuteRules;
                \[Tau]Bfrom\[Tau]=\[Tau]Bfrom\[Tau][[Ordering[dPSol]]];
                tfrom\[Tau]B=Simplify[tfrom\[Tau]B/.\[Tau]BPermuteRules];
                Vol\[Tau]B=Simplify[Vol\[Tau]B/.\[Tau]BPermuteRules];
                
                TB=Map[Function[u,Map[Coefficient[u,#]&,\[Tau]]][#]&,\[Tau]Bfrom\[Tau]];
                (*tfrom\[Tau]BTerms=Map[Map[Join[{FactorTermsList[#][[1]]},Exponent[#,Join[t,\[Tau]B]]]&,{Expand[#]}/.Plus->Sequence]&,tfrom\[Tau]B];
                Vol\[Tau]BTerms=Map[Join[{FactorTermsList[#][[1]]},Exponent[#,Join[t,\[Tau]B]]]&,{Expand[Vol\[Tau]B]}/.Plus->Sequence];*)
                NDiagTerms=Count[Vol\[Tau]BTerms,_?(Sort[#[[-h11;;]]]==Join[Table[0,{i,h11-1}],{3/2}]&)];
                dPSol=dPSol[[Ordering[dPSol]]];
                NL=h11-Count[dPSol,_?(#>=0&)];
                
                If[FormatString,
                    (*Return[{"RMAT4CYCLE"->ToString[TB,InputForm],"2FROMROT4CYCLES"->ToString[tfrom\[Tau]BTerms,InputForm],"VOLFROMROT4CYCLES"->ToString[Vol\[Tau]BTerms,InputForm],"NDIAGTERMS"->NDiagTerms,"DIVTYPES"->ToString[dPSol,InputForm],"NLARGE"->NL}];*)
                    Return[{"RMAT4CYCLE"->ToString[TB,InputForm],"2FROMROT4CYCLES"->ToString[FullForm[tfrom\[Tau]B]],"VOLFROMROT4CYCLES"->ToString[FullForm[Vol\[Tau]B]],"NDIAGTERMS"->NDiagTerms,"DIVTYPES"->ToString[dPSol,InputForm],"NLARGE"->NL}];
                ,
                    (*Return[{"RMAT4CYCLE"->TB,"2FROMROT4CYCLES"->tfrom\[Tau]BTerms,"VOLFROMROT4CYCLES"->Vol\[Tau]BTerms,"NDIAGTERMS"->NDiagTerms,"DIVTYPES"->dPSol,"NLARGE"->NL}];*)
                    Return[{"RMAT4CYCLE"->TB,"2FROMROT4CYCLES"->FullForm[tfrom\[Tau]B],"VOLFROMROT4CYCLES"->FullForm[Vol\[Tau]B],"NDIAGTERMS"->NDiagTerms,"DIVTYPES"->dPSol,"NLARGE"->NL}];
                ];
            ,
                Return[{}];
            ];
        ,
            Return[{}];
        ];
    ,
        Return[{}];
    ];
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
Basis="BASIS"/.Geometry;
InvBasis="INVBASIS"/.Geometry;
KahlerMat="KAHLERMAT"/.Geometry;

result=ExplicitCheck[ITensXJ,Basis,InvBasis,KahlerMat];
SwissCheeseIDField=Thread[{"H11","POLYID","GEOMN"}->{H11,PolyID,GeomN}];
If[Length[result]==0,
    WriteString[$Output,"None"];
,
    SwissCheeseDoc=StringRulestoJSON[Join[result,SwissCheeseIDField]];
    WriteString[$Output,"+EXPLICITCHEESE.{\"POLYID\":"<>ToString[PolyID]<>",\"GEOMN\":"<>ToString[GeomN]<>"}>"<>SwissCheeseDoc];
]
WriteString[$Output,"\n"];

Exit[];
