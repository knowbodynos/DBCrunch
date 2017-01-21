(* ::Package:: *)

$HistoryLength=0;

WorkingPath/:Set[WorkingPath,_]:=(ClearAll[WorkingPath];WorkingPath=$CommandLine[[5]]);
IntermediateName/:Set[IntermediateName,_]:=(ClearAll[IntermediateName];IntermediateName=$CommandLine[[6]]);

Get["cohomCalgKoszulExtensionSilent`"];
(*Get["ToricCY`"];*)


OrientedPolynomials[ResCWS_,Invol_]:=Module[{TDivs,TDivs2Inds,InvolGroups,DiffVecs,DiffKernel,CoordTerms,MonomialTerms,BinomialTerms,PolynomialTerms,PolynomialsSplit,NewResCWSSplit,i},
    TDivs=Table[ToExpression["D"<>ToString[i]],{i,Length[ResCWS]}];
    (*TDivs2Inds=Map[#\[Rule]First[Flatten[Position[TDivs,#,1]]]&,TDivs];*)
     InvolGroups=DeleteDuplicates[Invol/.Rule->List,(Union@#1)==(Union@#2)&];
    (*DiffVecs=Transpose[Map[Subtract@@#&,InvolGroups/.Thread[TDivs\[Rule]ResCWS]]];
    DiffKernel=NullSpace[DiffVecs];
    BinomialTerms=Map[Map[Times@@#&,Transpose[MapThread[Thread[If[#2<0,Reverse[#1],#1]^Abs[#2]]&,{InvolGroups,#}]]]&,DiffKernel];*)

    CoordTerms=Map[{#}&,Complement[TDivs,Union@@InvolGroups]];

    MonomialTerms=Map[{Times@@#}&,InvolGroups];

    BinomialTerms=DeleteDuplicates[DeleteCases[Flatten[Map[Function[u,Map[Map[Times@@#&,Transpose[MapThread[Thread[If[#2<0,Reverse[#1],#1]^Abs[#2]]&,{u,#}]]]&,NullSpace[Transpose[Map[Subtract@@ResCWS[[#]]&,u/.Thread[TDivs->Range[Length[TDivs]]]]]]]][#]&,Rest[Subsets[InvolGroups]]],1],{}]];
    PolynomialTerms=Map[Union@@#&,Gather[Join[CoordTerms,MonomialTerms,BinomialTerms],Length[Intersection[#1,#2]]>0&]];
    PolynomialsSplit=Table[Flatten[Map[Function[u,Map[#.u&,Select[DeleteDuplicates[Tuples[{1,-1},Length[u]],#1==-#2&],i*(#.u)==#.(u/.Invol)&]]][#]&,PolynomialTerms],1],{i,{1,-1}}];
    (*/.Thread[TDivs->Table[ToExpression[CoordName<>ToString[i]],{i,Length[TDivs]}]];*)
    NewResCWSSplit=Map[Map[{#/.Plus->Sequence}[[1]]/.Join[{Times->Plus,Power->Times},Thread[TDivs->ResCWS]]&,#]&,PolynomialsSplit];
    Return[{PolynomialsSplit,NewResCWSSplit}];
];


SGCD[x_]:=Module[{gcd},
    gcd=GCD[x];
    If[gcd==0,1,gcd];
];
GetMaxLoci[Loci_]:=DeleteDuplicates[SortBy[Loci,Length],MemberQ[{#1,#2},Intersection[#1,#2]]&];

(*TorusFixedLocusInds[x_,y_]:=Module[{gcd,maxloci},
    gcd=GCD@@x[[y]];
    If[gcd\[Equal]0,
        maxloci={};
    ,
        maxloci=MaxLoci[Table[Pick[Range[Length[x]],MapIndexed[((!EvenQ[#1])&&(!IntersectingQ[y,#2]))||(EvenQ[#1]&&IntersectingQ[y,#2])&,(x*n)/gcd]],{n,1,2*gcd,2}]];
    ];
    Return[maxloci];
];*)

(*GetTorusFixedLoci[WeightsSplit_,TDivsSplit_]:=Module[{gcd,maxloci},
    gcd=GCD@@WeightsSplit[[2]];
    If[gcd\[Equal]0,
        maxloci={};
    ,
        maxloci=GetMaxLoci[Table[Pick[Join@@TDivsSplit,Join[Map[!EvenQ[#]&,(WeightsSplit[[1]]*n)/gcd],Map[!OddQ[#]&,(WeightsSplit[[2]]*n)/gcd]]],{n,1,2*gcd,2}]];
    ];
    Return[maxloci];
];

GetAllFixedLoci[ResCWSSplit_,TDivsSplit_]:=GetMaxLoci[Join[Map[{#}&,TDivsSplit[[2]]],Join@@Map[GetTorusFixedLoci[#,TDivsSplit]&,Transpose[Transpose/@ResCWSSplit]]]];*)

(*GetAllMaxFixedLoci[ResCWSSplit_,TDivsSplit_]:=Module[{CC,DD,IndRanges,ZeroInds,Coeffs,i,PermVectors,SymCond,ASymCond,CCDDSol,TorusZeroDivSets,ASymDivSets,ZeroDivSets,MaxZeroDivSets,MaxTorusZeroDivSets,TorusCoeffInds,MaxASymDivSets},
    CC=Table[ToExpression["cc"<>ToString[i]],{i,Length[ResCWSSplit[[1,1]]]}];
    DD=TakeDrop[Table[ToExpression["dd"<>ToString[i]],{i,Length[Join@@ResCWSSplit]}],Length[ResCWSSplit[[1]]]];
    IndRanges=TakeDrop[Range[Length[Join@@ResCWSSplit]],Length[ResCWSSplit[[1]]]];
    ZeroInds=Select[Subsets[Join@@IndRanges],!SubsetQ[Intersection[IndRanges[[2]],#],IndRanges[[2]]]&];
    Coeffs={};
    For[i=1,i\[LessEqual]Length[ZeroInds],i++,
        PermVectors=TakeDrop[Table[Boole[MemberQ[ZeroInds[[i]],j]],{j,Length[Join@@ResCWSSplit]}],Length[ResCWSSplit[[1]]]];
        SymCond=MapThread[If[#2\[Equal]0,(#1/(GCD@@(CC.Transpose[ResCWSSplit[[2]]])))\[Equal](2*#3)+#2,(#1/(GCD@@(CC.Transpose[ResCWSSplit[[2]]])))\[NotEqual](2*#3)+1-#2]&,{(CC.Transpose[ResCWSSplit[[1]]]),PermVectors[[1]],DD[[1]]}];
        ASymCond=MapThread[If[#2\[Equal]0,(#1/(GCD@@(CC.Transpose[ResCWSSplit[[2]]])))\[Equal](2*#3)+1-#2,(#1/(GCD@@(CC.Transpose[ResCWSSplit[[2]]])))\[NotEqual](2*#3)+#2]&,{(CC.Transpose[ResCWSSplit[[2]]]),PermVectors[[2]],DD[[2]]}];
        CCDDSol=Quiet[FindInstance[(And@@SymCond)&&(And@@ASymCond)&&(And@@Thread[CC\[GreaterEqual]0]),Join[CC,Join@@DD],Integers]];
        If[Length[CCDDSol]>0,
            ZeroInds=Select[ZeroInds,((#!=ZeroInds[[i]])&&(Intersection[ZeroInds[[i]],#]!=ZeroInds[[i]]))||(#==ZeroInds[[i]])&];
            Coeffs=Join[Coeffs,CC/.CCDDSol];
        ,
            ZeroInds=Join[ZeroInds[[;;i-1]],ZeroInds[[i+1;;]]];
            i--;
        ];
    ];
    TorusZeroDivSets=ZeroInds/.Thread[(Join@@IndRanges)\[Rule](Join@@TDivsSplit)];
    ASymDivSets={TDivsSplit[[2]]};
    ZeroDivSets=Join[ASymDivSets,TorusZeroDivSets];
    MaxZeroDivSets=GetMaxLoci[ZeroDivSets];
    MaxTorusZeroDivSets=Select[MaxZeroDivSets,MemberQ[TorusZeroDivSets,#]&];
    TorusCoeffInds=Map[Coeffs[[Sequence@@Flatten[Position[TorusZeroDivSets,#]]]]&,MaxTorusZeroDivSets];
    MaxASymDivSets=Complement[MaxZeroDivSets,MaxTorusZeroDivSets];
    Return[{MaxASymDivSets,MaxTorusZeroDivSets,TorusCoeffInds}];
];*)

GetMaxTorusFixedLoci[NewResCWSSplit_,NewTDivsSplit_]:=Module[{PosList,PosSubsets,i,m,A,AA,rankAA,j,ns,AAug,IsSol,TorusFixedLoci},
    PosList=Join@@MapIndexed[Thread[{#2[[1]],Range[Length[#]]}]&,NewResCWSSplit];
    PosSubsets=Select[Reverse[Subsets[PosList]],Count[#,_?(#[[1]]==2&)]>0&];
    i=1;
    While[i<=Length[PosSubsets],
        ns=Tuples[Map[Range[0,Plus@@NewResCWSSplit[[Sequence@@#]]]&,PosSubsets[[i]]]];
        m=Transpose[PosSubsets[[i]]][[1]]-1;
        A=Map[NewResCWSSplit[[Sequence@@#]]&,PosSubsets[[i]]];
        AA=Transpose[MapThread[#1/#2&,{Transpose[A],SGCD@@@Transpose[Map[NewResCWSSplit[[Sequence@@#]]&,Select[PosSubsets[[i]],#[[1]]==2&]]]}]];
        rankAA=MatrixRank[AA];
        j=1;
        While[j<=Length[ns],
            AAug=Transpose[Join[Transpose[AA],{(2*ns[[j]])+m}]];
            If[MatrixRank[AAug]==rankAA,
                Break[];
            ];
            j++;
        ];
        IsSol=(j<=Length[ns]);
        (*m=Transpose[PosSubsets[[i]]][[1]]-1;
        A=Map[NewResCWSSplit[[Sequence@@#]]&,PosSubsets[[i]]];
        AA=Transpose[MapThread[#1/#2&,{Transpose[A],SGCD@@@Transpose[Map[NewResCWSSplit[[Sequence@@#]]&,Select[PosSubsets[[i]],#[[1]]==2&]]]}]];
        n=Table[ToExpression["n"<>ToString[j]],{j,Length[PosSubsets[[i]]]}];
        AAug=Transpose[Join[Transpose[AA],{(2*n)+m}]];       
        Sols=FindInstance[(MatrixRank[AAug]==MatrixRank[AA])&&(And@@Thread[n>=0])&&(And@@Thread[n<=Map[Plus@@NewResCWSSplit[[Sequence@@#]]&,PosSubsets[[i]]]]),n,Integers];
        IsSol=(Length[r]>0);*)
        If[IsSol,
            PosSubsets=Select[PosSubsets,(#==PosSubsets[[i]])||(Intersection[#,PosSubsets[[i]]]!=#)&];
            i++;
        ,
            PosSubsets=Drop[PosSubsets,{i}];
        ];
    ];
    TorusFixedLoci=Map[Complement[Join@@NewTDivsSplit,Map[NewTDivsSplit[[Sequence@@#]]&,#]]&,PosSubsets];
    Return[TorusFixedLoci];
];

GetAllMaxFixedLoci[ResCWSSplit_,TDivsSplit_]:=GetMaxLoci[Join[{TDivsSplit[[2]]},GetMaxTorusFixedLoci[ResCWSSplit,TDivsSplit]]];


FixedLoci[ResCWS_,Invol_,SRIdeal_,FormatString_:True]:=Module[{TDivs,OrientPolysSplit,NewResCWSSplit,NewTDivsSplit,TDivRepl,CC,SolsP,MonomsP,SymMonomsP,AllFixedLoci,SRSectors,CompIntersection,AllSRTransversalFixedLoci,TotalGroebnerSRSector,TotalGroebner,AllFixedLociSubsets,AllFixedLociSubset,FlagSubset,PartialGroebnerSRSector,PartialGroebner,AllOSpaces,i,j,k},
    TDivs=Table[ToExpression["D"<>ToString[i]],{i,Length[ResCWS]}];
    {OrientPolysSplit,NewResCWSSplit}=OrientedPolynomials[ResCWS,Invol];

    NewTDivsSplit=MapThread[Table[ToExpression[#1<>ToString[i]],{i,Length[#2]}]&,{{"E","F"},OrientPolysSplit}];
    TDivRepl=Map[#[[1]]->#[[2]]&,Transpose[{Join@@NewTDivsSplit,Join@@OrientPolysSplit}]];

    CC=Table[ToExpression["CC"<>ToString[i]],{i,Length[Join@@NewResCWSSplit]}];
    SolsP=CC/.Solve[((Transpose[Join@@NewResCWSSplit].CC)==(Plus@@ResCWS))&&(And@@Thread[CC>=0]),CC,Integers];
    
    MonomsP=Map[Times@@Thread[(Join@@NewTDivsSplit)^#]&,SolsP];
    SymMonomsP=Select[MonomsP,And@@(EvenQ@Exponent[#,NewTDivsSplit[[2]]])&];
    (*MonomsPSplit={SymMonomsP,Complement[MonomsP,SymMonomsP]};*)
    (*SolsQ=CC/.Solve[(Transpose[NewResCWSRows].CC==(Plus@@NewResCWSRows)-(Plus@@ResCWSRows))&&(And@@Thread[CC>=0]),CC,Integers];
    MonomsQ=Map[Times@@Thread[NewTDivs^#]&,SolsQ];
    DD=Table[ToExpression["DD"<>ToString[i]],{i,Length[MonomsQ]}];
    SolsCSQ=NullSpace[Map[Thread[Coefficient[#,DD]]&,DeleteCases[Flatten[CoefficientList[((DD.MonomsQ)/.TDivRepl),TDivs]],0]]];
    PolynomsQ=Select[Map[#.MonomsQ&,SolsCSQ],And@@(EvenQ@Exponent[#,ASymNewTDivs])&];*)

    (*TorusFixedLoci=Select[Map[Function[u,(Or@@(ASymNewTDivs/.u))&&Select[Complement[NewTDivs,ASymNewTDivs],#/.u&]][#]&,ASymCoords],#=!=False&];*)
    AllFixedLoci=GetAllMaxFixedLoci[NewResCWSSplit,NewTDivsSplit];

    SRSectors=Union[Map[Map[#-1&,Union[#]]&,Tuples[SRIdeal/.Times->List]]];
    (*CompIntersection=Join[{Plus@@SymMonomsP},PolynomsQ];*)
    CompIntersection={Plus@@SymMonomsP};
    AllSRTransversalFixedLoci=DeleteCases[DeleteDuplicates[Table[
        TotalGroebnerSRSector=GroebnerBasis[Join[CompIntersection,AllFixedLoci[[i]],SRSectors[[1]]]/.TDivRepl,TDivs];
        TotalGroebner={TotalGroebnerSRSector};
        j=2;
        While[(j<=Length[SRSectors])&&(TotalGroebnerSRSector=={1}),
            TotalGroebnerSRSector=GroebnerBasis[Join[CompIntersection,AllFixedLoci[[i]],SRSectors[[j]]]/.TDivRepl,TDivs];
            TotalGroebner=Join[TotalGroebner,{TotalGroebnerSRSector}];
            j++;
        ];
        If[TotalGroebnerSRSector=={1},
            {}
        ,
            AllFixedLociSubsets=Subsets[AllFixedLoci[[i]]][[2;;]];
            FlagSubset=False;
            j=1;
            While[(j<=Length[AllFixedLociSubsets])&&(!FlagSubset),
                AllFixedLociSubset=AllFixedLociSubsets[[j]];
                PartialGroebnerSRSector=GroebnerBasis[Join[CompIntersection,AllFixedLociSubset,SRSectors[[1]]]/.TDivRepl,TDivs];
                k=2;
                While[(k<=Length[TotalGroebner])&&(Length[PartialGroebnerSRSector]==Length[TotalGroebner[[k-1]]]),
                    PartialGroebnerSRSector=GroebnerBasis[Join[CompIntersection,AllFixedLociSubset,SRSectors[[k]]]/.TDivRepl,TDivs];
                    k++;
                ];
                If[Length[PartialGroebnerSRSector]==Length[TotalGroebner[[k-1]]],
                    TotalGroebnerSRSector=TotalGroebner[[k-1]];
                    While[(k<=Length[SRSectors])&&(Length[PartialGroebnerSRSector]==Length[TotalGroebnerSRSector]),
                        TotalGroebnerSRSector=GroebnerBasis[Join[CompIntersection,AllFixedLoci[[i]],SRSectors[[k]]]/.TDivRepl,TDivs];
                        PartialGroebnerSRSector=GroebnerBasis[Join[CompIntersection,AllFixedLociSubset,SRSectors[[k]]]/.TDivRepl,TDivs];
                        k++;
                    ];
                    If[Length[PartialGroebnerSRSector]==Length[TotalGroebnerSRSector],
                        FlagSubset=True;
                    ];
                ];
                j++;
            ];
            If[FlagSubset,
                AllFixedLociSubset
            ,
                {}
            ]
        ]
    ,{i,Length[AllFixedLoci]}]],{}];
    If[FormatString,
        AllOSpaces=Map[("O"<>ToString[9-(2*Length[#[[1]]])])->ToString[#,InputForm]&,GatherBy[AllSRTransversalFixedLoci,Length]/.TDivRepl];
    ,
        AllOSpaces=Map[("O"<>ToString[9-(2*Length[#[[1]]])])->#&,GatherBy[AllSRTransversalFixedLoci,Length]/.TDivRepl];
    ];
    Return[AllOSpaces];
];


HodgeSplit[H11_,H21_,Invol_,Basis_,DResVerts_,ResCWS_,FormatString_:True]:=Module[{TDivs,InvolGroups,t,ILin,EpDivs,EmDivs,ISplit,J,ILinSplit,JReduced,tIdeal,SymJ,SymJCoeffsVars,SymH11,SymH21,H11Split,H21Split,Splitting},
    TDivs=Table[ToExpression["D"<>ToString[i]],{i,Length[ResCWS]}];
    InvolGroups=DeleteDuplicates[Invol/.Rule->List,(Union@#1)==(Union@#2)&];
    t=Table[ToExpression["t"<>ToString[i]],{i,H11}];

    ILin=Plus@@(DResVerts*TDivs);
    EpDivs=Table[ToExpression["Ep"<>ToString[i]],{i,Length[InvolGroups]}];
    EmDivs=Table[ToExpression["Em"<>ToString[i]],{i,Length[InvolGroups]}];
    ISplit=Join@@Table[{InvolGroups[[i,1]]-(1/2)*(EpDivs[[i]]+EmDivs[[i]]),InvolGroups[[i,2]]-(1/2)*(EpDivs[[i]]-EmDivs[[i]])},{i,Length[InvolGroups]}];
    J=t.TDivs[[Basis]];
    ILinSplit=GroebnerBasis[Join[ILin,ISplit],Variables[{ILin,ISplit}]];
    JReduced=PolynomialReduce[J,ILinSplit,Join[TDivs,EpDivs,EmDivs]][[2]];
    tIdeal=GroebnerBasis[Map[Coefficient[JReduced,#]&,EmDivs],t];
    SymJ=PolynomialReduce[JReduced,tIdeal,Join[TDivs,EpDivs,EmDivs]][[2]];
    SymJCoeffsVars=Transpose[Select[MapThread[{Coefficient[SymJ,#1],#2}&,{Join[TDivs,EpDivs],Join[TDivs,Plus@@@InvolGroups]}],#[[1]]=!=0&]];
    SymH11=Length[SymJCoeffsVars[[1]]];
    SymH21=((H21-H11)/2)-SymH11;
    H11Split={SymH11,H11-SymH11};
    H21Split={SymH21,H21-SymH21};
    If[FormatString,
        Splitting={"H11SPLIT"->ToString[H11Split,InputForm],"H21SPLIT"->ToString[H21Split,InputForm],"COEFFSVARS"->ToString[SymJCoeffsVars,InputForm]};
    ,
        Splitting={"H11SPLIT"->H11Split,"H21SPLIT"->H21Split,"COEFFSVARS"->SymJCoeffsVars};
    ];
    Return[Splitting];
];

AllBasesHodgeSplit[H11_,H21_,Invol_,DResVerts_,ResCWS_,FormatString_:True]:=Module[{Bases,HodgeSplitResults,SameResultQ,Splitting},
    Bases=Map[#&,Select[Subsets[Range[Length[ResCWS]],{MatrixRank[ResCWS]}],MatrixRank[ResCWS[[#]]]==MatrixRank[ResCWS]&]];
    HodgeSplitResults=Map[HodgeSplit[H11,H21,Invol,#,DResVerts,ResCWS,False]&,Bases];
    SameResultQ=Equal@@(ReleaseHold[Hold[{"H11SPLIT","COEFFSVARS"[[2]]}]/.HodgeSplitResults]);
    Splitting={"ALLBASES"->SameResultQ};
    If[FormatString,
        Splitting=Join[Splitting,Thread[{"H11SPLIT","H21SPLIT"}->(ReleaseHold[Hold[{ToString["H11SPLIT",InputForm],ToString["H21SPLIT",InputForm]}]/.HodgeSplitResults[[1]]])]];
    ,
        Splitting=Join[Splitting,Thread[{"H11SPLIT","H21SPLIT"}->({"H11SPLIT","H21SPLIT"}/.HodgeSplitResults[[1]])]];
    ];
    Return[Splitting];
];


(*MongoDirac=MongoClient[$CommandLine[[7]]];
ToricCYDirac=MongoDirac@getDB["ToricCY"];*)
(*TimeLimit=ToExpression[$CommandLine[[8]]];
MemoryLimit=ToExpression[$CommandLine[[9]]];
SkippedFile=$CommandLine[[10]];*)
Geometry=Map[#[[1]]->ToExpression[#[[2]]]&,ToExpression[$CommandLine[[7]]]];

PolyID="POLYID"/.Geometry;
GeomN="GEOMN"/.Geometry;
TriangN="TRIANGN"/.Geometry;
H11="H11"/.Geometry;
H21="H21"/.Geometry;
DResVerts="DRESVERTS"/.Geometry;
ResCWS=Transpose["RESCWS"/.Geometry];
SRIdeal="SRIDEAL"/.Geometry;
InvolN="INVOLN"/.Geometry;
Invol="INVOL"/.Geometry;

(*timemem=TimeConstrained[
    MemoryConstrained[
        {AbsoluteTiming[MaxMemoryUsed[fixedlociresult=FixedLoci[ResCWS,Invol,SRIdeal,True]]],AbsoluteTiming[MaxMemoryUsed[hodgesplitresult=AllBasesHodgeSplit[H11,H21,Invol,DResVerts,ResCWS,True]]]}
    ,MemoryLimit,"MemorySkipped"]
,TimeLimit,"TimeSkipped"];*)

(*result=TimeConstrained[
    MemoryConstrained[
        Join[FixedLoci[ResCWS,Invol,SRIdeal,True],AllBasesHodgeSplit[H11,H21,Invol,DResVerts,ResCWS,True]]
    ,MemoryLimit,"MemorySkipped"]
,TimeLimit,"TimeSkipped"];

If[!MemberQ[{"TimeSkipped","MemorySkipped"},result],
    (*{fixedlocistats,hodgesplitstats}=timemem;
    {fixedlocitime,fixedlocimem}=fixedlocistats;
    {hodgesplittime,hodgesplitmem}=hodgesplitstats;
    result=Join[fixedlociresult,hodgesplitresult];*)
    InvolIDField=Thread[{"H11","POLYID","GEOMN","TRIANGN","INVOLN"}->{H11,PolyID,GeomN,TriangN,InvolN}];
    NewInvolFields=Select[result,#[[1]]!="ALLBASES"&];
    outresult=Join[InvolIDField,result];
    
    (ToricCYDirac@getCollection["INVOL"])@update[StringRulestoJSONJava@InvolIDField,StringRulestoJSONJava@{"$set"->NewInvolFields}];
    storage=BSONSize[NewInvolFields];
	output=StringReplace[StringRulestoJSON[outresult],{" "->""}];
    (*WriteString[$Output,"Fixed Loci Time: "<>ToString[fixedlocitime]<>"\n"];
    WriteString[$Output,"Hodge Split Time: "<>ToString[hodgesplittime]<>"\n"];
    WriteString[$Output,"Total Time: "<>ToString[fixedlocitime+hodgesplittime]<>"\n"];
    WriteString[$Output,"Fixed Loci Max Memory: "<>ToString[fixedlocimem]<>"\n"];
    WriteString[$Output,"Hodge Split Max Memory: "<>ToString[hodgesplitmem]<>"\n"];
    WriteString[$Output,"Total Max Memory: "<>ToString[Max[fixedlocimem,hodgesplitmem]]<>"\n"];
    WriteString[$Output,"Total Storage: "<>ToString[storage]<>"\n"];*)
,
	(*output=timemem;*)
    output=result;
    WriteString[SkippedFile,ToString[Row[{PolyID,"_",GeomN,"_",TriangN,"_",InvolN," ",output,"\n"}],InputForm]];
];*)

result=Join[FixedLoci[ResCWS,Invol,SRIdeal,True],AllBasesHodgeSplit[H11,H21,Invol,DResVerts,ResCWS,True]];
InvolIDField=Thread[{"H11","POLYID","GEOMN","TRIANGN","INVOLN"}->{H11,PolyID,GeomN,TriangN,InvolN}];
NewInvolFields=Select[result,#[[1]]!="ALLBASES"&];
outresult={Join[InvolIDField,result]};
    
(*(ToricCYDirac@getCollection["INVOL"])@update[StringRulestoJSONJava@InvolIDField,StringRulestoJSONJava@{"$set"->NewInvolFields}];*)
outputlist=Map[StringReplace[ExportString[#,"JSON","Compact"->True],{" "->""}]&,outresult];
output=(StringJoin@@Table[If[i>1,"\n",""]<>"Output: "<>outputlist[[i]],{i,Length[outputlist]}]);

WriteString[$Output,output<>"\n"];
(*DeleteDirectory[WorkingPath<>"/"<>IntermediateName,DeleteContents\[Rule]True];*)
MongoDirac@close[];
Exit[];
