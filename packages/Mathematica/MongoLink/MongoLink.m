(* ::Package:: *)

(*Convert JSON document to compressed Mathematica rules document.*)
JSONtoStringRules[doc_]:=ImportString[doc,"JSON"];

(*Convert compressed Mathematica rules document to JSON document.*)
StringRulestoJSON[doc_]:=StringReplace[ExportString[doc,"JSON","Compact"->True],{" "->""}];

(*Convert expanded Mathematica rules document to compressed Mathematica rules document.*)
ExpressionRulestoStringRules[doc_]:=Module[{PosExpressionsToStrings,StringsResult},
    (*PosExpressionsToStrings=Select[Map[Most[#]&,Position[doc,Rule]],((Head[doc[[Sequence@@#]][[1]]]===String)&&(Head[doc[[Sequence@@#]][[2]]]=!=Integer))&];*)
    PosExpressionsToStrings=Position[doc,_?((Head[#]===Rule)&&(Head[#[[1]]]===String)&&(Head[#[[2]]]=!=Integer)&)];
    StringsResult=MapAt[Rule[#[[1]],ToString[#[[2]],InputForm]]&,doc,PosExpressionsToStrings];
    Return[StringsResult];
];

(*Convert expanded Mathematica rules document to JSON document.*)
ExpressionRulestoJSON[doc_]:=StringRulestoJSON[ExpressionRulestoStringRules[doc]];

(*Convert compressed Mathematica rules document to expanded Mathematica rules document.*)
StringRulestoExpressionRules[doc_]:=Module[{PosStringsToExpressions,ExpressionsResult},
    PosStringsToExpressions=Position[doc,_?((Head[#]===Rule)&&(Quiet[Check[ToExpression[#[[2]]],False]]=!=False)&)];
    ExpressionsResult=MapAt[Rule[#[[1]],ToExpression[#[[2]]]]&,doc,PosStringsToExpressions];
    Return[ExpressionsResult];
];

(*Convert JSON document to expanded Mathematica rules document.*)
JSONtoExpressionRules[doc_]:=StringRulestoExpressionRules[JSONtoStringRules[doc]];
