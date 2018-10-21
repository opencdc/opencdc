package deltad.core;

import java.util.Properties;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class GBaseExpressionVisitor extends DefaultExpressionVisitor {
	private final boolean trimDoubleQuotation;
	
	public GBaseExpressionVisitor(boolean trim) {
		this.trimDoubleQuotation = trim;
	}
	
	public GBaseExpressionVisitor() {
		this.trimDoubleQuotation = true;
	}
	
	private final Properties whereMap = new Properties();

	private void resolveAndExpression(Expression expression) {

		// System.out.println("andExpression = " + expression.toString());
		if (expression instanceof EqualsTo) {
			EqualsTo et = (EqualsTo) expression;
			resolveEqualsTo(et);
		} else if (expression instanceof AndExpression) {
			AndExpression andExpression = (AndExpression) expression;
			//System.out.println("LeftExpression = " + andExpression.getLeftExpression());
			// System.out.println("RightExpression = " + andExpression.getRightExpression());
			resolveAndExpression(andExpression.getLeftExpression());
			resolveAndExpression(andExpression.getRightExpression());
			//System.out.println("after resolveAnd = " + expression.toString());
		} else if (expression instanceof IsNullExpression) {
			IsNullExpression isNullExp = (IsNullExpression)expression;
			if (isNullExp.getLeftExpression() instanceof Column) {
				Column col = new Column();
				String left = trimDoubleQuotation ? SqlRedoProcessor.trimDoubleQuotation(isNullExp.getLeftExpression().toString()) : isNullExp.getLeftExpression().toString();
				col.setColumnName(left);
				col.setTable(new Table(null, null));
				isNullExp.setLeftExpression(col);
				whereMap.setProperty(left, "IS NULL");
				//System.out.println(isNullExp.getLeftExpression().getClass().getName());
			}
		}
	}

	@Override
	public void visit(AndExpression andExpression) {
		resolveAndExpression(andExpression);
	}
	
	public Properties getWhereMap() {
		return this.whereMap;
	}

	private void resolveEqualsTo(EqualsTo equalsTo) {
		//System.out.println("left= " + equalsTo.getLeftExpression());
		//System.out.println("right= " + equalsTo.getRightExpression());
		String left = equalsTo.getLeftExpression().toString();
		String right = equalsTo.getRightExpression().toString();
		Column col = new Column();
		String leftString = trimDoubleQuotation ? SqlRedoProcessor.trimDoubleQuotation(left) : left;
		col.setColumnName(leftString);
		
		col.setTable(new Table(null, null));
		equalsTo.setLeftExpression(col);
		whereMap.setProperty(leftString, right);
		//System.out.println(equalsTo.toString());
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		resolveEqualsTo(equalsTo);
	}

}
