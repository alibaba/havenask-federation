/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// ANTLR GENERATED CODE: DO NOT EDIT
/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.painless.antlr;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
abstract class PainlessLexer extends Lexer {
  static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    WS=1, COMMENT=2, LBRACK=3, RBRACK=4, LBRACE=5, RBRACE=6, LP=7, RP=8, DOT=9,
    NSDOT=10, COMMA=11, SEMICOLON=12, IF=13, IN=14, ELSE=15, WHILE=16, DO=17,
    FOR=18, CONTINUE=19, BREAK=20, RETURN=21, NEW=22, TRY=23, CATCH=24, THROW=25,
    THIS=26, INSTANCEOF=27, BOOLNOT=28, BWNOT=29, MUL=30, DIV=31, REM=32,
    ADD=33, SUB=34, LSH=35, RSH=36, USH=37, LT=38, LTE=39, GT=40, GTE=41,
    EQ=42, EQR=43, NE=44, NER=45, BWAND=46, XOR=47, BWOR=48, BOOLAND=49, BOOLOR=50,
    COND=51, COLON=52, ELVIS=53, REF=54, ARROW=55, FIND=56, MATCH=57, INCR=58,
    DECR=59, ASSIGN=60, AADD=61, ASUB=62, AMUL=63, ADIV=64, AREM=65, AAND=66,
    AXOR=67, AOR=68, ALSH=69, ARSH=70, AUSH=71, OCTAL=72, HEX=73, INTEGER=74,
    DECIMAL=75, STRING=76, REGEX=77, TRUE=78, FALSE=79, NULL=80, PRIMITIVE=81,
    DEF=82, ID=83, DOTINTEGER=84, DOTID=85;
  public static final int AFTER_DOT = 1;
  public static String[] modeNames = {
    "DEFAULT_MODE", "AFTER_DOT"
  };

  public static final String[] ruleNames = {
    "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", "DOT",
    "NSDOT", "COMMA", "SEMICOLON", "IF", "IN", "ELSE", "WHILE", "DO", "FOR",
    "CONTINUE", "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "THIS",
    "INSTANCEOF", "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", "LSH",
    "RSH", "USH", "LT", "LTE", "GT", "GTE", "EQ", "EQR", "NE", "NER", "BWAND",
    "XOR", "BWOR", "BOOLAND", "BOOLOR", "COND", "COLON", "ELVIS", "REF", "ARROW",
    "FIND", "MATCH", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV",
    "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", "HEX",
    "INTEGER", "DECIMAL", "STRING", "REGEX", "TRUE", "FALSE", "NULL", "PRIMITIVE",
    "DEF", "ID", "DOTINTEGER", "DOTID"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "'?.'",
    "','", "';'", "'if'", "'in'", "'else'", "'while'", "'do'", "'for'", "'continue'",
    "'break'", "'return'", "'new'", "'try'", "'catch'", "'throw'", "'this'",
    "'instanceof'", "'!'", "'~'", "'*'", "'/'", "'%'", "'+'", "'-'", "'<<'",
    "'>>'", "'>>>'", "'<'", "'<='", "'>'", "'>='", "'=='", "'==='", "'!='",
    "'!=='", "'&'", "'^'", "'|'", "'&&'", "'||'", "'?'", "':'", "'?:'", "'::'",
    "'->'", "'=~'", "'==~'", "'++'", "'--'", "'='", "'+='", "'-='", "'*='",
    "'/='", "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null,
    null, null, null, null, null, "'true'", "'false'", "'null'", null, "'def'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP",
    "DOT", "NSDOT", "COMMA", "SEMICOLON", "IF", "IN", "ELSE", "WHILE", "DO",
    "FOR", "CONTINUE", "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW",
    "THIS", "INSTANCEOF", "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD",
    "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", "GTE", "EQ", "EQR", "NE",
    "NER", "BWAND", "XOR", "BWOR", "BOOLAND", "BOOLOR", "COND", "COLON", "ELVIS",
    "REF", "ARROW", "FIND", "MATCH", "INCR", "DECR", "ASSIGN", "AADD", "ASUB",
    "AMUL", "ADIV", "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH",
    "OCTAL", "HEX", "INTEGER", "DECIMAL", "STRING", "REGEX", "TRUE", "FALSE",
    "NULL", "PRIMITIVE", "DEF", "ID", "DOTINTEGER", "DOTID"
  };
  public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

  /**
   * @deprecated Use {@link #VOCABULARY} instead.
   */
  @Deprecated
  public static final String[] tokenNames;
  static {
    tokenNames = new String[_SYMBOLIC_NAMES.length];
    for (int i = 0; i < tokenNames.length; i++) {
      tokenNames[i] = VOCABULARY.getLiteralName(i);
      if (tokenNames[i] == null) {
        tokenNames[i] = VOCABULARY.getSymbolicName(i);
      }

      if (tokenNames[i] == null) {
        tokenNames[i] = "<INVALID>";
      }
    }
  }

  @Override
  @Deprecated
  public String[] getTokenNames() {
    return tokenNames;
  }

  @Override

  public Vocabulary getVocabulary() {
    return VOCABULARY;
  }


  /** Is the preceding {@code /} a the beginning of a regex (true) or a division (false). */
  protected abstract boolean isSlashRegex();


  public PainlessLexer(CharStream input) {
    super(input);
    _interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @Override
  public String getGrammarFileName() { return "PainlessLexer.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public String[] getModeNames() { return modeNames; }

  @Override
  public ATN getATN() { return _ATN; }

  @Override
  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 30:
      return DIV_sempred((RuleContext)_localctx, predIndex);
    case 76:
      return REGEX_sempred((RuleContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean DIV_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return  isSlashRegex() == false ;
    }
    return true;
  }
  private boolean REGEX_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 1:
      return  isSlashRegex() ;
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2W\u027a\b\1\b\1\4"+
    "\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n"+
    "\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
    "\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
    "\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
    " \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
    "+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
    "\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t"+
    "=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4"+
    "I\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\t"+
    "T\4U\tU\4V\tV\3\2\6\2\u00b0\n\2\r\2\16\2\u00b1\3\2\3\2\3\3\3\3\3\3\3\3"+
    "\7\3\u00ba\n\3\f\3\16\3\u00bd\13\3\3\3\3\3\3\3\3\3\3\3\7\3\u00c4\n\3\f"+
    "\3\16\3\u00c7\13\3\3\3\3\3\5\3\u00cb\n\3\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3"+
    "\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3"+
    "\f\3\f\3\r\3\r\3\16\3\16\3\16\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20"+
    "\3\21\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\24"+
    "\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\25\3\25"+
    "\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\27\3\30\3\30\3\30"+
    "\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\3\33"+
    "\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34"+
    "\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3 \3!\3!\3\"\3\"\3#\3#\3$\3"+
    "$\3$\3%\3%\3%\3&\3&\3&\3&\3\'\3\'\3(\3(\3(\3)\3)\3*\3*\3*\3+\3+\3+\3,"+
    "\3,\3,\3,\3-\3-\3-\3.\3.\3.\3.\3/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3\62"+
    "\3\63\3\63\3\63\3\64\3\64\3\65\3\65\3\66\3\66\3\66\3\67\3\67\3\67\38\3"+
    "8\38\39\39\39\3:\3:\3:\3:\3;\3;\3;\3<\3<\3<\3=\3=\3>\3>\3>\3?\3?\3?\3"+
    "@\3@\3@\3A\3A\3A\3B\3B\3B\3C\3C\3C\3D\3D\3D\3E\3E\3E\3F\3F\3F\3F\3G\3"+
    "G\3G\3G\3H\3H\3H\3H\3H\3I\3I\6I\u01ba\nI\rI\16I\u01bb\3I\5I\u01bf\nI\3"+
    "J\3J\3J\6J\u01c4\nJ\rJ\16J\u01c5\3J\5J\u01c9\nJ\3K\3K\3K\7K\u01ce\nK\f"+
    "K\16K\u01d1\13K\5K\u01d3\nK\3K\5K\u01d6\nK\3L\3L\3L\7L\u01db\nL\fL\16"+
    "L\u01de\13L\5L\u01e0\nL\3L\3L\6L\u01e4\nL\rL\16L\u01e5\5L\u01e8\nL\3L"+
    "\3L\5L\u01ec\nL\3L\6L\u01ef\nL\rL\16L\u01f0\5L\u01f3\nL\3L\5L\u01f6\n"+
    "L\3M\3M\3M\3M\3M\3M\7M\u01fe\nM\fM\16M\u0201\13M\3M\3M\3M\3M\3M\3M\3M"+
    "\7M\u020a\nM\fM\16M\u020d\13M\3M\5M\u0210\nM\3N\3N\3N\3N\6N\u0216\nN\r"+
    "N\16N\u0217\3N\3N\7N\u021c\nN\fN\16N\u021f\13N\3N\3N\3O\3O\3O\3O\3O\3"+
    "P\3P\3P\3P\3P\3P\3Q\3Q\3Q\3Q\3Q\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3"+
    "R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3"+
    "R\3R\3R\5R\u0259\nR\3S\3S\3S\3S\3T\3T\7T\u0261\nT\fT\16T\u0264\13T\3U"+
    "\3U\3U\7U\u0269\nU\fU\16U\u026c\13U\5U\u026e\nU\3U\3U\3V\3V\7V\u0274\n"+
    "V\fV\16V\u0277\13V\3V\3V\7\u00bb\u00c5\u01ff\u020b\u0217\2W\4\3\6\4\b"+
    "\5\n\6\f\7\16\b\20\t\22\n\24\13\26\f\30\r\32\16\34\17\36\20 \21\"\22$"+
    "\23&\24(\25*\26,\27.\30\60\31\62\32\64\33\66\348\35:\36<\37> @!B\"D#F"+
    "$H%J&L\'N(P)R*T+V,X-Z.\\/^\60`\61b\62d\63f\64h\65j\66l\67n8p9r:t;v<x="+
    "z>|?~@\u0080A\u0082B\u0084C\u0086D\u0088E\u008aF\u008cG\u008eH\u0090I"+
    "\u0092J\u0094K\u0096L\u0098M\u009aN\u009cO\u009eP\u00a0Q\u00a2R\u00a4"+
    "S\u00a6T\u00a8U\u00aaV\u00acW\4\2\3\25\5\2\13\f\17\17\"\"\4\2\f\f\17\17"+
    "\3\2\629\4\2NNnn\4\2ZZzz\5\2\62;CHch\3\2\63;\3\2\62;\b\2FFHHNNffhhnn\4"+
    "\2GGgg\4\2--//\6\2FFHHffhh\4\2$$^^\4\2))^^\3\2\f\f\4\2\f\f\61\61\t\2W"+
    "Weekknouuwwzz\5\2C\\aac|\6\2\62;C\\aac|\u02a0\2\4\3\2\2\2\2\6\3\2\2\2"+
    "\2\b\3\2\2\2\2\n\3\2\2\2\2\f\3\2\2\2\2\16\3\2\2\2\2\20\3\2\2\2\2\22\3"+
    "\2\2\2\2\24\3\2\2\2\2\26\3\2\2\2\2\30\3\2\2\2\2\32\3\2\2\2\2\34\3\2\2"+
    "\2\2\36\3\2\2\2\2 \3\2\2\2\2\"\3\2\2\2\2$\3\2\2\2\2&\3\2\2\2\2(\3\2\2"+
    "\2\2*\3\2\2\2\2,\3\2\2\2\2.\3\2\2\2\2\60\3\2\2\2\2\62\3\2\2\2\2\64\3\2"+
    "\2\2\2\66\3\2\2\2\28\3\2\2\2\2:\3\2\2\2\2<\3\2\2\2\2>\3\2\2\2\2@\3\2\2"+
    "\2\2B\3\2\2\2\2D\3\2\2\2\2F\3\2\2\2\2H\3\2\2\2\2J\3\2\2\2\2L\3\2\2\2\2"+
    "N\3\2\2\2\2P\3\2\2\2\2R\3\2\2\2\2T\3\2\2\2\2V\3\2\2\2\2X\3\2\2\2\2Z\3"+
    "\2\2\2\2\\\3\2\2\2\2^\3\2\2\2\2`\3\2\2\2\2b\3\2\2\2\2d\3\2\2\2\2f\3\2"+
    "\2\2\2h\3\2\2\2\2j\3\2\2\2\2l\3\2\2\2\2n\3\2\2\2\2p\3\2\2\2\2r\3\2\2\2"+
    "\2t\3\2\2\2\2v\3\2\2\2\2x\3\2\2\2\2z\3\2\2\2\2|\3\2\2\2\2~\3\2\2\2\2\u0080"+
    "\3\2\2\2\2\u0082\3\2\2\2\2\u0084\3\2\2\2\2\u0086\3\2\2\2\2\u0088\3\2\2"+
    "\2\2\u008a\3\2\2\2\2\u008c\3\2\2\2\2\u008e\3\2\2\2\2\u0090\3\2\2\2\2\u0092"+
    "\3\2\2\2\2\u0094\3\2\2\2\2\u0096\3\2\2\2\2\u0098\3\2\2\2\2\u009a\3\2\2"+
    "\2\2\u009c\3\2\2\2\2\u009e\3\2\2\2\2\u00a0\3\2\2\2\2\u00a2\3\2\2\2\2\u00a4"+
    "\3\2\2\2\2\u00a6\3\2\2\2\2\u00a8\3\2\2\2\3\u00aa\3\2\2\2\3\u00ac\3\2\2"+
    "\2\4\u00af\3\2\2\2\6\u00ca\3\2\2\2\b\u00ce\3\2\2\2\n\u00d0\3\2\2\2\f\u00d2"+
    "\3\2\2\2\16\u00d4\3\2\2\2\20\u00d6\3\2\2\2\22\u00d8\3\2\2\2\24\u00da\3"+
    "\2\2\2\26\u00de\3\2\2\2\30\u00e3\3\2\2\2\32\u00e5\3\2\2\2\34\u00e7\3\2"+
    "\2\2\36\u00ea\3\2\2\2 \u00ed\3\2\2\2\"\u00f2\3\2\2\2$\u00f8\3\2\2\2&\u00fb"+
    "\3\2\2\2(\u00ff\3\2\2\2*\u0108\3\2\2\2,\u010e\3\2\2\2.\u0115\3\2\2\2\60"+
    "\u0119\3\2\2\2\62\u011d\3\2\2\2\64\u0123\3\2\2\2\66\u0129\3\2\2\28\u012e"+
    "\3\2\2\2:\u0139\3\2\2\2<\u013b\3\2\2\2>\u013d\3\2\2\2@\u013f\3\2\2\2B"+
    "\u0142\3\2\2\2D\u0144\3\2\2\2F\u0146\3\2\2\2H\u0148\3\2\2\2J\u014b\3\2"+
    "\2\2L\u014e\3\2\2\2N\u0152\3\2\2\2P\u0154\3\2\2\2R\u0157\3\2\2\2T\u0159"+
    "\3\2\2\2V\u015c\3\2\2\2X\u015f\3\2\2\2Z\u0163\3\2\2\2\\\u0166\3\2\2\2"+
    "^\u016a\3\2\2\2`\u016c\3\2\2\2b\u016e\3\2\2\2d\u0170\3\2\2\2f\u0173\3"+
    "\2\2\2h\u0176\3\2\2\2j\u0178\3\2\2\2l\u017a\3\2\2\2n\u017d\3\2\2\2p\u0180"+
    "\3\2\2\2r\u0183\3\2\2\2t\u0186\3\2\2\2v\u018a\3\2\2\2x\u018d\3\2\2\2z"+
    "\u0190\3\2\2\2|\u0192\3\2\2\2~\u0195\3\2\2\2\u0080\u0198\3\2\2\2\u0082"+
    "\u019b\3\2\2\2\u0084\u019e\3\2\2\2\u0086\u01a1\3\2\2\2\u0088\u01a4\3\2"+
    "\2\2\u008a\u01a7\3\2\2\2\u008c\u01aa\3\2\2\2\u008e\u01ae\3\2\2\2\u0090"+
    "\u01b2\3\2\2\2\u0092\u01b7\3\2\2\2\u0094\u01c0\3\2\2\2\u0096\u01d2\3\2"+
    "\2\2\u0098\u01df\3\2\2\2\u009a\u020f\3\2\2\2\u009c\u0211\3\2\2\2\u009e"+
    "\u0222\3\2\2\2\u00a0\u0227\3\2\2\2\u00a2\u022d\3\2\2\2\u00a4\u0258\3\2"+
    "\2\2\u00a6\u025a\3\2\2\2\u00a8\u025e\3\2\2\2\u00aa\u026d\3\2\2\2\u00ac"+
    "\u0271\3\2\2\2\u00ae\u00b0\t\2\2\2\u00af\u00ae\3\2\2\2\u00b0\u00b1\3\2"+
    "\2\2\u00b1\u00af\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2\u00b3\3\2\2\2\u00b3"+
    "\u00b4\b\2\2\2\u00b4\5\3\2\2\2\u00b5\u00b6\7\61\2\2\u00b6\u00b7\7\61\2"+
    "\2\u00b7\u00bb\3\2\2\2\u00b8\u00ba\13\2\2\2\u00b9\u00b8\3\2\2\2\u00ba"+
    "\u00bd\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bb\u00b9\3\2\2\2\u00bc\u00be\3\2"+
    "\2\2\u00bd\u00bb\3\2\2\2\u00be\u00cb\t\3\2\2\u00bf\u00c0\7\61\2\2\u00c0"+
    "\u00c1\7,\2\2\u00c1\u00c5\3\2\2\2\u00c2\u00c4\13\2\2\2\u00c3\u00c2\3\2"+
    "\2\2\u00c4\u00c7\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c5\u00c3\3\2\2\2\u00c6"+
    "\u00c8\3\2\2\2\u00c7\u00c5\3\2\2\2\u00c8\u00c9\7,\2\2\u00c9\u00cb\7\61"+
    "\2\2\u00ca\u00b5\3\2\2\2\u00ca\u00bf\3\2\2\2\u00cb\u00cc\3\2\2\2\u00cc"+
    "\u00cd\b\3\2\2\u00cd\7\3\2\2\2\u00ce\u00cf\7}\2\2\u00cf\t\3\2\2\2\u00d0"+
    "\u00d1\7\177\2\2\u00d1\13\3\2\2\2\u00d2\u00d3\7]\2\2\u00d3\r\3\2\2\2\u00d4"+
    "\u00d5\7_\2\2\u00d5\17\3\2\2\2\u00d6\u00d7\7*\2\2\u00d7\21\3\2\2\2\u00d8"+
    "\u00d9\7+\2\2\u00d9\23\3\2\2\2\u00da\u00db\7\60\2\2\u00db\u00dc\3\2\2"+
    "\2\u00dc\u00dd\b\n\3\2\u00dd\25\3\2\2\2\u00de\u00df\7A\2\2\u00df\u00e0"+
    "\7\60\2\2\u00e0\u00e1\3\2\2\2\u00e1\u00e2\b\13\3\2\u00e2\27\3\2\2\2\u00e3"+
    "\u00e4\7.\2\2\u00e4\31\3\2\2\2\u00e5\u00e6\7=\2\2\u00e6\33\3\2\2\2\u00e7"+
    "\u00e8\7k\2\2\u00e8\u00e9\7h\2\2\u00e9\35\3\2\2\2\u00ea\u00eb\7k\2\2\u00eb"+
    "\u00ec\7p\2\2\u00ec\37\3\2\2\2\u00ed\u00ee\7g\2\2\u00ee\u00ef\7n\2\2\u00ef"+
    "\u00f0\7u\2\2\u00f0\u00f1\7g\2\2\u00f1!\3\2\2\2\u00f2\u00f3\7y\2\2\u00f3"+
    "\u00f4\7j\2\2\u00f4\u00f5\7k\2\2\u00f5\u00f6\7n\2\2\u00f6\u00f7\7g\2\2"+
    "\u00f7#\3\2\2\2\u00f8\u00f9\7f\2\2\u00f9\u00fa\7q\2\2\u00fa%\3\2\2\2\u00fb"+
    "\u00fc\7h\2\2\u00fc\u00fd\7q\2\2\u00fd\u00fe\7t\2\2\u00fe\'\3\2\2\2\u00ff"+
    "\u0100\7e\2\2\u0100\u0101\7q\2\2\u0101\u0102\7p\2\2\u0102\u0103\7v\2\2"+
    "\u0103\u0104\7k\2\2\u0104\u0105\7p\2\2\u0105\u0106\7w\2\2\u0106\u0107"+
    "\7g\2\2\u0107)\3\2\2\2\u0108\u0109\7d\2\2\u0109\u010a\7t\2\2\u010a\u010b"+
    "\7g\2\2\u010b\u010c\7c\2\2\u010c\u010d\7m\2\2\u010d+\3\2\2\2\u010e\u010f"+
    "\7t\2\2\u010f\u0110\7g\2\2\u0110\u0111\7v\2\2\u0111\u0112\7w\2\2\u0112"+
    "\u0113\7t\2\2\u0113\u0114\7p\2\2\u0114-\3\2\2\2\u0115\u0116\7p\2\2\u0116"+
    "\u0117\7g\2\2\u0117\u0118\7y\2\2\u0118/\3\2\2\2\u0119\u011a\7v\2\2\u011a"+
    "\u011b\7t\2\2\u011b\u011c\7{\2\2\u011c\61\3\2\2\2\u011d\u011e\7e\2\2\u011e"+
    "\u011f\7c\2\2\u011f\u0120\7v\2\2\u0120\u0121\7e\2\2\u0121\u0122\7j\2\2"+
    "\u0122\63\3\2\2\2\u0123\u0124\7v\2\2\u0124\u0125\7j\2\2\u0125\u0126\7"+
    "t\2\2\u0126\u0127\7q\2\2\u0127\u0128\7y\2\2\u0128\65\3\2\2\2\u0129\u012a"+
    "\7v\2\2\u012a\u012b\7j\2\2\u012b\u012c\7k\2\2\u012c\u012d\7u\2\2\u012d"+
    "\67\3\2\2\2\u012e\u012f\7k\2\2\u012f\u0130\7p\2\2\u0130\u0131\7u\2\2\u0131"+
    "\u0132\7v\2\2\u0132\u0133\7c\2\2\u0133\u0134\7p\2\2\u0134\u0135\7e\2\2"+
    "\u0135\u0136\7g\2\2\u0136\u0137\7q\2\2\u0137\u0138\7h\2\2\u01389\3\2\2"+
    "\2\u0139\u013a\7#\2\2\u013a;\3\2\2\2\u013b\u013c\7\u0080\2\2\u013c=\3"+
    "\2\2\2\u013d\u013e\7,\2\2\u013e?\3\2\2\2\u013f\u0140\7\61\2\2\u0140\u0141"+
    "\6 \2\2\u0141A\3\2\2\2\u0142\u0143\7\'\2\2\u0143C\3\2\2\2\u0144\u0145"+
    "\7-\2\2\u0145E\3\2\2\2\u0146\u0147\7/\2\2\u0147G\3\2\2\2\u0148\u0149\7"+
    ">\2\2\u0149\u014a\7>\2\2\u014aI\3\2\2\2\u014b\u014c\7@\2\2\u014c\u014d"+
    "\7@\2\2\u014dK\3\2\2\2\u014e\u014f\7@\2\2\u014f\u0150\7@\2\2\u0150\u0151"+
    "\7@\2\2\u0151M\3\2\2\2\u0152\u0153\7>\2\2\u0153O\3\2\2\2\u0154\u0155\7"+
    ">\2\2\u0155\u0156\7?\2\2\u0156Q\3\2\2\2\u0157\u0158\7@\2\2\u0158S\3\2"+
    "\2\2\u0159\u015a\7@\2\2\u015a\u015b\7?\2\2\u015bU\3\2\2\2\u015c\u015d"+
    "\7?\2\2\u015d\u015e\7?\2\2\u015eW\3\2\2\2\u015f\u0160\7?\2\2\u0160\u0161"+
    "\7?\2\2\u0161\u0162\7?\2\2\u0162Y\3\2\2\2\u0163\u0164\7#\2\2\u0164\u0165"+
    "\7?\2\2\u0165[\3\2\2\2\u0166\u0167\7#\2\2\u0167\u0168\7?\2\2\u0168\u0169"+
    "\7?\2\2\u0169]\3\2\2\2\u016a\u016b\7(\2\2\u016b_\3\2\2\2\u016c\u016d\7"+
    "`\2\2\u016da\3\2\2\2\u016e\u016f\7~\2\2\u016fc\3\2\2\2\u0170\u0171\7("+
    "\2\2\u0171\u0172\7(\2\2\u0172e\3\2\2\2\u0173\u0174\7~\2\2\u0174\u0175"+
    "\7~\2\2\u0175g\3\2\2\2\u0176\u0177\7A\2\2\u0177i\3\2\2\2\u0178\u0179\7"+
    "<\2\2\u0179k\3\2\2\2\u017a\u017b\7A\2\2\u017b\u017c\7<\2\2\u017cm\3\2"+
    "\2\2\u017d\u017e\7<\2\2\u017e\u017f\7<\2\2\u017fo\3\2\2\2\u0180\u0181"+
    "\7/\2\2\u0181\u0182\7@\2\2\u0182q\3\2\2\2\u0183\u0184\7?\2\2\u0184\u0185"+
    "\7\u0080\2\2\u0185s\3\2\2\2\u0186\u0187\7?\2\2\u0187\u0188\7?\2\2\u0188"+
    "\u0189\7\u0080\2\2\u0189u\3\2\2\2\u018a\u018b\7-\2\2\u018b\u018c\7-\2"+
    "\2\u018cw\3\2\2\2\u018d\u018e\7/\2\2\u018e\u018f\7/\2\2\u018fy\3\2\2\2"+
    "\u0190\u0191\7?\2\2\u0191{\3\2\2\2\u0192\u0193\7-\2\2\u0193\u0194\7?\2"+
    "\2\u0194}\3\2\2\2\u0195\u0196\7/\2\2\u0196\u0197\7?\2\2\u0197\177\3\2"+
    "\2\2\u0198\u0199\7,\2\2\u0199\u019a\7?\2\2\u019a\u0081\3\2\2\2\u019b\u019c"+
    "\7\61\2\2\u019c\u019d\7?\2\2\u019d\u0083\3\2\2\2\u019e\u019f\7\'\2\2\u019f"+
    "\u01a0\7?\2\2\u01a0\u0085\3\2\2\2\u01a1\u01a2\7(\2\2\u01a2\u01a3\7?\2"+
    "\2\u01a3\u0087\3\2\2\2\u01a4\u01a5\7`\2\2\u01a5\u01a6\7?\2\2\u01a6\u0089"+
    "\3\2\2\2\u01a7\u01a8\7~\2\2\u01a8\u01a9\7?\2\2\u01a9\u008b\3\2\2\2\u01aa"+
    "\u01ab\7>\2\2\u01ab\u01ac\7>\2\2\u01ac\u01ad\7?\2\2\u01ad\u008d\3\2\2"+
    "\2\u01ae\u01af\7@\2\2\u01af\u01b0\7@\2\2\u01b0\u01b1\7?\2\2\u01b1\u008f"+
    "\3\2\2\2\u01b2\u01b3\7@\2\2\u01b3\u01b4\7@\2\2\u01b4\u01b5\7@\2\2\u01b5"+
    "\u01b6\7?\2\2\u01b6\u0091\3\2\2\2\u01b7\u01b9\7\62\2\2\u01b8\u01ba\t\4"+
    "\2\2\u01b9\u01b8\3\2\2\2\u01ba\u01bb\3\2\2\2\u01bb\u01b9\3\2\2\2\u01bb"+
    "\u01bc\3\2\2\2\u01bc\u01be\3\2\2\2\u01bd\u01bf\t\5\2\2\u01be\u01bd\3\2"+
    "\2\2\u01be\u01bf\3\2\2\2\u01bf\u0093\3\2\2\2\u01c0\u01c1\7\62\2\2\u01c1"+
    "\u01c3\t\6\2\2\u01c2\u01c4\t\7\2\2\u01c3\u01c2\3\2\2\2\u01c4\u01c5\3\2"+
    "\2\2\u01c5\u01c3\3\2\2\2\u01c5\u01c6\3\2\2\2\u01c6\u01c8\3\2\2\2\u01c7"+
    "\u01c9\t\5\2\2\u01c8\u01c7\3\2\2\2\u01c8\u01c9\3\2\2\2\u01c9\u0095\3\2"+
    "\2\2\u01ca\u01d3\7\62\2\2\u01cb\u01cf\t\b\2\2\u01cc\u01ce\t\t\2\2\u01cd"+
    "\u01cc\3\2\2\2\u01ce\u01d1\3\2\2\2\u01cf\u01cd\3\2\2\2\u01cf\u01d0\3\2"+
    "\2\2\u01d0\u01d3\3\2\2\2\u01d1\u01cf\3\2\2\2\u01d2\u01ca\3\2\2\2\u01d2"+
    "\u01cb\3\2\2\2\u01d3\u01d5\3\2\2\2\u01d4\u01d6\t\n\2\2\u01d5\u01d4\3\2"+
    "\2\2\u01d5\u01d6\3\2\2\2\u01d6\u0097\3\2\2\2\u01d7\u01e0\7\62\2\2\u01d8"+
    "\u01dc\t\b\2\2\u01d9\u01db\t\t\2\2\u01da\u01d9\3\2\2\2\u01db\u01de\3\2"+
    "\2\2\u01dc\u01da\3\2\2\2\u01dc\u01dd\3\2\2\2\u01dd\u01e0\3\2\2\2\u01de"+
    "\u01dc\3\2\2\2\u01df\u01d7\3\2\2\2\u01df\u01d8\3\2\2\2\u01e0\u01e7\3\2"+
    "\2\2\u01e1\u01e3\5\24\n\2\u01e2\u01e4\t\t\2\2\u01e3\u01e2\3\2\2\2\u01e4"+
    "\u01e5\3\2\2\2\u01e5\u01e3\3\2\2\2\u01e5\u01e6\3\2\2\2\u01e6\u01e8\3\2"+
    "\2\2\u01e7\u01e1\3\2\2\2\u01e7\u01e8\3\2\2\2\u01e8\u01f2\3\2\2\2\u01e9"+
    "\u01eb\t\13\2\2\u01ea\u01ec\t\f\2\2\u01eb\u01ea\3\2\2\2\u01eb\u01ec\3"+
    "\2\2\2\u01ec\u01ee\3\2\2\2\u01ed\u01ef\t\t\2\2\u01ee\u01ed\3\2\2\2\u01ef"+
    "\u01f0\3\2\2\2\u01f0\u01ee\3\2\2\2\u01f0\u01f1\3\2\2\2\u01f1\u01f3\3\2"+
    "\2\2\u01f2\u01e9\3\2\2\2\u01f2\u01f3\3\2\2\2\u01f3\u01f5\3\2\2\2\u01f4"+
    "\u01f6\t\r\2\2\u01f5\u01f4\3\2\2\2\u01f5\u01f6\3\2\2\2\u01f6\u0099\3\2"+
    "\2\2\u01f7\u01ff\7$\2\2\u01f8\u01f9\7^\2\2\u01f9\u01fe\7$\2\2\u01fa\u01fb"+
    "\7^\2\2\u01fb\u01fe\7^\2\2\u01fc\u01fe\n\16\2\2\u01fd\u01f8\3\2\2\2\u01fd"+
    "\u01fa\3\2\2\2\u01fd\u01fc\3\2\2\2\u01fe\u0201\3\2\2\2\u01ff\u0200\3\2"+
    "\2\2\u01ff\u01fd\3\2\2\2\u0200\u0202\3\2\2\2\u0201\u01ff\3\2\2\2\u0202"+
    "\u0210\7$\2\2\u0203\u020b\7)\2\2\u0204\u0205\7^\2\2\u0205\u020a\7)\2\2"+
    "\u0206\u0207\7^\2\2\u0207\u020a\7^\2\2\u0208\u020a\n\17\2\2\u0209\u0204"+
    "\3\2\2\2\u0209\u0206\3\2\2\2\u0209\u0208\3\2\2\2\u020a\u020d\3\2\2\2\u020b"+
    "\u020c\3\2\2\2\u020b\u0209\3\2\2\2\u020c\u020e\3\2\2\2\u020d\u020b\3\2"+
    "\2\2\u020e\u0210\7)\2\2\u020f\u01f7\3\2\2\2\u020f\u0203\3\2\2\2\u0210"+
    "\u009b\3\2\2\2\u0211\u0215\7\61\2\2\u0212\u0213\7^\2\2\u0213\u0216\n\20"+
    "\2\2\u0214\u0216\n\21\2\2\u0215\u0212\3\2\2\2\u0215\u0214\3\2\2\2\u0216"+
    "\u0217\3\2\2\2\u0217\u0218\3\2\2\2\u0217\u0215\3\2\2\2\u0218\u0219\3\2"+
    "\2\2\u0219\u021d\7\61\2\2\u021a\u021c\t\22\2\2\u021b\u021a\3\2\2\2\u021c"+
    "\u021f\3\2\2\2\u021d\u021b\3\2\2\2\u021d\u021e\3\2\2\2\u021e\u0220\3\2"+
    "\2\2\u021f\u021d\3\2\2\2\u0220\u0221\6N\3\2\u0221\u009d\3\2\2\2\u0222"+
    "\u0223\7v\2\2\u0223\u0224\7t\2\2\u0224\u0225\7w\2\2\u0225\u0226\7g\2\2"+
    "\u0226\u009f\3\2\2\2\u0227\u0228\7h\2\2\u0228\u0229\7c\2\2\u0229\u022a"+
    "\7n\2\2\u022a\u022b\7u\2\2\u022b\u022c\7g\2\2\u022c\u00a1\3\2\2\2\u022d"+
    "\u022e\7p\2\2\u022e\u022f\7w\2\2\u022f\u0230\7n\2\2\u0230\u0231\7n\2\2"+
    "\u0231\u00a3\3\2\2\2\u0232\u0233\7d\2\2\u0233\u0234\7q\2\2\u0234\u0235"+
    "\7q\2\2\u0235\u0236\7n\2\2\u0236\u0237\7g\2\2\u0237\u0238\7c\2\2\u0238"+
    "\u0259\7p\2\2\u0239\u023a\7d\2\2\u023a\u023b\7{\2\2\u023b\u023c\7v\2\2"+
    "\u023c\u0259\7g\2\2\u023d\u023e\7u\2\2\u023e\u023f\7j\2\2\u023f\u0240"+
    "\7q\2\2\u0240\u0241\7t\2\2\u0241\u0259\7v\2\2\u0242\u0243\7e\2\2\u0243"+
    "\u0244\7j\2\2\u0244\u0245\7c\2\2\u0245\u0259\7t\2\2\u0246\u0247\7k\2\2"+
    "\u0247\u0248\7p\2\2\u0248\u0259\7v\2\2\u0249\u024a\7n\2\2\u024a\u024b"+
    "\7q\2\2\u024b\u024c\7p\2\2\u024c\u0259\7i\2\2\u024d\u024e\7h\2\2\u024e"+
    "\u024f\7n\2\2\u024f\u0250\7q\2\2\u0250\u0251\7c\2\2\u0251\u0259\7v\2\2"+
    "\u0252\u0253\7f\2\2\u0253\u0254\7q\2\2\u0254\u0255\7w\2\2\u0255\u0256"+
    "\7d\2\2\u0256\u0257\7n\2\2\u0257\u0259\7g\2\2\u0258\u0232\3\2\2\2\u0258"+
    "\u0239\3\2\2\2\u0258\u023d\3\2\2\2\u0258\u0242\3\2\2\2\u0258\u0246\3\2"+
    "\2\2\u0258\u0249\3\2\2\2\u0258\u024d\3\2\2\2\u0258\u0252\3\2\2\2\u0259"+
    "\u00a5\3\2\2\2\u025a\u025b\7f\2\2\u025b\u025c\7g\2\2\u025c\u025d\7h\2"+
    "\2\u025d\u00a7\3\2\2\2\u025e\u0262\t\23\2\2\u025f\u0261\t\24\2\2\u0260"+
    "\u025f\3\2\2\2\u0261\u0264\3\2\2\2\u0262\u0260\3\2\2\2\u0262\u0263\3\2"+
    "\2\2\u0263\u00a9\3\2\2\2\u0264\u0262\3\2\2\2\u0265\u026e\7\62\2\2\u0266"+
    "\u026a\t\b\2\2\u0267\u0269\t\t\2\2\u0268\u0267\3\2\2\2\u0269\u026c\3\2"+
    "\2\2\u026a\u0268\3\2\2\2\u026a\u026b\3\2\2\2\u026b\u026e\3\2\2\2\u026c"+
    "\u026a\3\2\2\2\u026d\u0265\3\2\2\2\u026d\u0266\3\2\2\2\u026e\u026f\3\2"+
    "\2\2\u026f\u0270\bU\4\2\u0270\u00ab\3\2\2\2\u0271\u0275\t\23\2\2\u0272"+
    "\u0274\t\24\2\2\u0273\u0272\3\2\2\2\u0274\u0277\3\2\2\2\u0275\u0273\3"+
    "\2\2\2\u0275\u0276\3\2\2\2\u0276\u0278\3\2\2\2\u0277\u0275\3\2\2\2\u0278"+
    "\u0279\bV\4\2\u0279\u00ad\3\2\2\2$\2\3\u00b1\u00bb\u00c5\u00ca\u01bb\u01be"+
    "\u01c5\u01c8\u01cf\u01d2\u01d5\u01dc\u01df\u01e5\u01e7\u01eb\u01f0\u01f2"+
    "\u01f5\u01fd\u01ff\u0209\u020b\u020f\u0215\u0217\u021d\u0258\u0262\u026a"+
    "\u026d\u0275\5\b\2\2\4\3\2\4\2\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
