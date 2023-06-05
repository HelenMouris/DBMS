package engine;

public class SQLTerm {

    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;

    public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue){
        this._objValue = _objValue;
        this._strColumnName = _strColumnName;
        this._strOperator = _strOperator;
        this._strTableName = _strTableName;
    }

}
