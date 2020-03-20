package com.wdw.hive.jdbchandler.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

/**
 * 目的：获取AST中的表，列，以及对其所做的操作，如SELECT,INSERT 重点：获取SELECT操作中的表和列的相关操作。其他操作这判断到表级别。
 * 实现思路：对AST深度优先遍历，遇到操作的token则判断当前的操作， 遇到TOK_TAB或TOK_TABREF则判断出当前操作的表，遇到子句则压栈当前处理，处理子句。 子句处理完，栈弹出。
 */
public class HiveParse {

  private static final LogUtil LOGGER = LogUtil.getLogger();
  private static final String UNKNOWN = "UNKNOWN";
  private Map<String, String> alias = new HashMap<String, String>();
  private Map<String, String> cols = new TreeMap<String, String>();
  private Map<String, String> colAlais = new TreeMap<String, String>();
  private Set<String> tables = new HashSet<String>();
  private Stack<String> tableNameStack = new Stack<String>();
  private Stack<Oper> operStack = new Stack<Oper>();
  private String nowQueryTable = "";
  private List<Oper> opers = new ArrayList<>();
  private Oper oper;
  private boolean joinClause = false;
  private boolean joinOn = true;
  private Pattern jarPattern = Pattern.compile("add jar (.*?);");
  Pattern functionPattern = Pattern.compile("create temporary function (.*?) as '(.*?)';");

  public enum Oper {
    SELECT, INSERT, DROP, TRUNCATE, LOAD, CREATETABLE, ALTER
  }

  public Set<String> parseIteral(ASTNode ast) {
    Set<String> set = new HashSet<String>();
    prepareToParseCurrentNodeAndChilds(ast);
    set.addAll(parseChildNodes(ast));
    set.addAll(parseCurrentNode(ast, set));
    endParseCurrentNode(ast);
    return set;
  }

  private void endParseCurrentNode(ASTNode ast) {
    if (ast.getToken() != null) {
      switch (ast.getToken().getType()) {//join 从句结束，跳出join
        case HiveParser.TOK_RIGHTOUTERJOIN:
        case HiveParser.TOK_LEFTOUTERJOIN:
        case HiveParser.TOK_JOIN:
          joinClause = false;
          break;
        case HiveParser.TOK_QUERY:
          break;
        case HiveParser.TOK_INSERT:
        case HiveParser.TOK_CREATETABLE:
          nowQueryTable = tableNameStack.pop();
          oper = operStack.pop();
          break;
        case HiveParser.TOK_SELECT:
          nowQueryTable = tableNameStack.pop();
          oper = operStack.pop();
          break;
        default:
          break;
      }
    }
  }

  private Set<String> parseCurrentNode(ASTNode ast, Set<String> set) {
    if (ast.getToken() != null) {
      switch (ast.getToken().getType()) {
        case HiveParser.TOK_CREATETABLE:
          ASTNode createTableName = (ASTNode) ast.getChild(0).getChild(0);
          tables.add(createTableName.getText() + "\t" + Oper.CREATETABLE);
          break;
        case HiveParser.TOK_TABLE_PARTITION:
//            case HiveParser.TOK_TABNAME:
          if (ast.getChildCount() != 2) {
            String table = BaseSemanticAnalyzer
                .getUnescapedName((ASTNode) ast.getChild(0));
            if (oper == Oper.SELECT) {
              nowQueryTable = table;
            }
            tables.add(table + "\t" + oper);
          }
          break;

        case HiveParser.TOK_TAB:// outputTable
          String tableTab = BaseSemanticAnalyzer
              .getUnescapedName((ASTNode) ast.getChild(0));
          if (oper == Oper.SELECT) {
            nowQueryTable = tableTab;
          }
          tables.add(tableTab + "\t" + oper);
          break;
        case HiveParser.TOK_TABREF:// inputTable
          ASTNode tabTree = (ASTNode) ast.getChild(0);
          String tableName = (tabTree.getChildCount() == 1) ? BaseSemanticAnalyzer
              .getUnescapedName((ASTNode) tabTree.getChild(0))
              : BaseSemanticAnalyzer
                  .getUnescapedName((ASTNode) tabTree.getChild(0))
                  + "." + tabTree.getChild(1);
          if (oper == Oper.SELECT) {
            if (joinClause && !"".equals(nowQueryTable)) {
              nowQueryTable += "&" + tableName;//
            } else {
              nowQueryTable = tableName;
            }
            set.add(tableName);
          }
          tables.add(tableName + "\t" + oper);
          if (ast.getChild(1) != null) {
            String alia = ast.getChild(1).getText().toLowerCase();
            alias.put(alia, tableName);//sql6 p别名在tabref只对应为一个表的别名。
          }
          break;
        case HiveParser.TOK_TABLE_OR_COL:
          if (ast.getParent().getType() != HiveParser.DOT) {
            String col = ast.getChild(0).getText().toLowerCase();
            if (alias.get(col) == null
                && colAlais.get(nowQueryTable + "." + col) == null) {
              if (nowQueryTable.indexOf("&") > 0) {//sql23
                cols.put(UNKNOWN + "." + col, "");
              } else {
                cols.put(nowQueryTable + "." + col, "");
              }
            }
          }
          break;
        case HiveParser.TOK_ALLCOLREF:
          cols.put(nowQueryTable + ".*", "");
          break;
        case HiveParser.TOK_SUBQUERY:
          if (ast.getChildCount() == 2) {
            String tableAlias = unescapeIdentifier(ast.getChild(1)
                .getText());
            String aliaReal = "";
            for (String table : set) {
              aliaReal += table + "&";
            }
            if (aliaReal.length() != 0) {
              aliaReal = aliaReal.substring(0, aliaReal.length() - 1);
            }
            alias.put(tableAlias, aliaReal);
          }
          break;

        case HiveParser.TOK_SELEXPR:
          if (ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
            String column = ast.getChild(0).getChild(0).getText()
                .toLowerCase();
            if (nowQueryTable.indexOf("&") > 0) {
              cols.put(UNKNOWN + "." + column, "");
            } else if (colAlais.get(nowQueryTable + "." + column) == null) {
              cols.put(nowQueryTable + "." + column, "");
            }
          } else if (ast.getChild(1) != null) {
            String columnAlia = ast.getChild(1).getText().toLowerCase();
            colAlais.put(nowQueryTable + "." + columnAlia, "");
          }
          break;
        case HiveParser.DOT:
          if (ast.getType() == HiveParser.DOT) {
            if (ast.getChildCount() == 2) {
              if (ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                  && ast.getChild(0).getChildCount() == 1
                  && ast.getChild(1).getType() == HiveParser.Identifier) {
                String alia = BaseSemanticAnalyzer
                    .unescapeIdentifier(ast.getChild(0)
                        .getChild(0).getText()
                        .toLowerCase());
                String column = BaseSemanticAnalyzer
                    .unescapeIdentifier(ast.getChild(1)
                        .getText().toLowerCase());
                String realTable = null;
                if (!tables.contains(alia + "\t" + oper)
                    && alias.get(alia) == null) {// [b SELECT, a
                  // SELECT]
                  alias.put(alia, nowQueryTable);
                }
                if (tables.contains(alia + "\t" + oper)) {
                  realTable = alia;
                } else if (alias.get(alia) != null) {
                  realTable = alias.get(alia);
                }
                if (realTable == null || realTable.length() == 0 || realTable.indexOf("&") > 0) {
                  realTable = UNKNOWN;
                }
                cols.put(realTable + "." + column, "");
              }
            }
          }
          break;
        case HiveParser.TOK_ALTERTABLE_ADDPARTS:
        case HiveParser.TOK_ALTERTABLE_RENAME:
        case HiveParser.TOK_ALTERTABLE_ADDCOLS:
          ASTNode alterTableName = (ASTNode) ast.getChild(0);
          tables.add(alterTableName.getText() + "\t" + oper);
          break;
        case HiveParser.TOK_RIGHTOUTERJOIN:
        case HiveParser.TOK_LEFTOUTERJOIN:
        case HiveParser.TOK_JOIN:
          if (ast.getChildCount() != 3) {
            joinOn = false;
          }
          break;
        case HiveParser.TOK_WHERE:
          ast.getType();
        default:
          break;
      }
    }
    return set;
  }

  private Set<String> parseChildNodes(ASTNode ast) {
    Set<String> set = new HashSet<String>();
    int numCh = ast.getChildCount();
    if (numCh > 0) {
      for (int num = 0; num < numCh; num++) {
        ASTNode child = (ASTNode) ast.getChild(num);
        set.addAll(parseIteral(child));
      }
    }
    return set;
  }

  private void prepareToParseCurrentNodeAndChilds(ASTNode ast) {
    if (ast.getToken() != null) {
      switch (ast.getToken().getType()) {
        //join 从句开始
        case HiveParser.TOK_RIGHTOUTERJOIN:
        case HiveParser.TOK_LEFTOUTERJOIN:
        case HiveParser.TOK_JOIN:
          joinClause = true;
          break;
        case HiveParser.TOK_CREATETABLE:
          operStack.push(oper);
          oper = Oper.CREATETABLE;
          opers.add(oper);
          break;
        case HiveParser.TOK_QUERY:
          tableNameStack.push(nowQueryTable);
          operStack.push(oper);
          nowQueryTable = "";//sql22
          oper = Oper.SELECT;
          opers.add(oper);
          break;
        case HiveParser.TOK_INSERT:
          tableNameStack.push(nowQueryTable);
          operStack.push(oper);
          oper = Oper.INSERT;
          opers.add(oper);
          break;
        case HiveParser.TOK_SELECT:
          tableNameStack.push(nowQueryTable);
          operStack.push(oper);
          oper = Oper.SELECT;
          break;
        case HiveParser.TOK_DROPTABLE:
          operStack.push(oper);
          oper = Oper.DROP;
          opers.add(oper);
          break;
        case HiveParser.TOK_TRUNCATETABLE:
          operStack.push(oper);
          oper = Oper.TRUNCATE;
          opers.add(oper);
          break;
        case HiveParser.TOK_LOAD:
          operStack.push(oper);
          oper = Oper.LOAD;
          opers.add(oper);
          break;
        case HiveParser.TOK_WHERE:
          break;
      }
      if (ast.getToken() != null
          && ast.getToken().getType() >= HiveParser.TOK_ALTERDATABASE_PROPERTIES
          && ast.getToken().getType() <= HiveParser.TOK_ALTERVIEW_RENAME) {
        oper = Oper.ALTER;
      }
    }
  }

  public static String unescapeIdentifier(String val) {
    if (val == null) {
      return null;
    }
    if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }

  private void output(Map<String, String> map) {
    Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> next = iterator.next();
      LOGGER.info(next.getKey() + "\t" + next.getValue());
    }
  }


  public Map<String, Object> parse(String parsesql) {
    Map<String, Object> map = new HashMap<>();
    try {
      parsesql = parsesql.toLowerCase();
      //解决udf问题
      String udfSql = "";
      Matcher m = jarPattern.matcher(parsesql);
      while (m.find()) {
        parsesql = parsesql.replace(m.group(), "");
      }

      Matcher m2 = functionPattern.matcher(parsesql);
      while (m2.find()) {
        parsesql = parsesql.replace(m2.group(), " ");
      }

      parsesql = parsesql.replaceAll("--(.*?)\n", "\n");
      parsesql = parsesql.replace("\n", " ").replace("from(", "from (")
          .replaceFirst("select (.*?) from ", "select 0 from ");
      parsesql = parsesql.replaceAll("\\`(.*?)\\`", "alias");
      if (parsesql.substring(parsesql.length() - 1).equals(";")) {
        parsesql = parsesql.substring(0, parsesql.length() - 1);
      }
      ParseDriver pd = new ParseDriver();
      ASTNode ast = null;
      LOGGER.info(parsesql);

      ast = pd.parse(parsesql);
      Set<String> strings = parseIteral(ast);

      map.put("tables", tables);
      map.put("opers", opers);
    } catch (Exception e) {
      LOGGER.error("解析sql结构异常，请检查sql语法", e);
      throw new RuntimeException(e.getMessage());
    }
    return map;
  }
}