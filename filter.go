package sql

import (
	"fmt"
	"time"

	"github.com/ioswarm/golik"
)

func NewFilter(cond golik.Condition) (string, error) {
	return interpretCondition(cond)
}

func interpretCondition(condition golik.Condition) (string, error) {
	switch condition.(type) {
	case golik.Operand:
		operand := condition.(golik.Operand)
		return interpretOperand(operand)
	case golik.Logic:
		logic := condition.(golik.Logic)
		return interpretLogical(logic)
	case golik.LogicNot:
		not := condition.(golik.LogicNot)
		return interpretNot(not)
	case golik.Grouping:
		grp := condition.(golik.Grouping)
		return interpretGrouping(grp)
	default:
		return "", fmt.Errorf("Unsupported condition %T", condition)
	}
}

func toSqlValue(value interface{}) interface{} {
	switch value.(type) {
	case string:
		return fmt.Sprintf("'%v'", value)
	case time.Time:
		t := value.(time.Time)
		return fmt.Sprintf("'%v'", t.Format("2006-01-02T15:04:05.000"))
	case *time.Time:
		t := value.(*time.Time)
		return fmt.Sprintf("'%v'", t.Format("2006-01-02T15:04:05.000"))
	default:
		return value
	}
}

func interpretOperand(op golik.Operand) (string, error) {
	value := toSqlValue(op.Value())

	switch op.Operator() {
	case golik.EQ:
		return fmt.Sprintln(op.Attribute(), "=", value), nil
	case golik.NE:
		return fmt.Sprintln(op.Attribute(), "!=", value), nil
	case golik.CO:
		return fmt.Sprintf("%v LIKE '%%v%'", op.Attribute(), op.Value()), nil
	case golik.SW:
		return fmt.Sprintf("%v LIKE '%v%'", op.Attribute(), op.Value()), nil
	case golik.EW:
		return fmt.Sprintf("%v LIKE '%%v'", op.Attribute(), op.Value()), nil
	case golik.PR:
		return fmt.Sprintln(op.Attribute(), "IS NOT NULL"), nil
	case golik.GT:
		return fmt.Sprintln(op.Attribute(), ">", value), nil
	case golik.GE:
		return fmt.Sprintln(op.Attribute(), ">=", value), nil
	case golik.LT:
		return fmt.Sprintln(op.Attribute(), "<", value), nil
	case golik.LE:
		return fmt.Sprintln(op.Attribute(), "<=", value), nil
	default:
		return "", fmt.Errorf("Unsupported operator %v", op.Operator())
	}
}

func interpretLogical(logic golik.Logic) (string, error) {
	switch logic.Logical() {
	case golik.AND:
		l, err := interpretCondition(logic.Left())
		if err != nil {
			return "", err
		}
		r, err := interpretCondition(logic.Right())
		if err != nil {
			return "", err
		}
		return fmt.Sprintln(l, "AND", r), nil
	case golik.OR:
		l, err := interpretCondition(logic.Left())
		if err != nil {
			return "", err
		}
		r, err := interpretCondition(logic.Right())
		if err != nil {
			return "", err
		}
		return fmt.Sprintln(l, "OR", r), nil
	default:
		return "", fmt.Errorf("Unsupported logical operator %v", logic.Logical())
	}
}

func interpretNot(not golik.LogicNot) (string, error) {
	inner, err := interpretCondition(not.InnerNot())
	if err != nil {
		return "", err
	}
	return fmt.Sprintln("not (", inner, ")"), nil
}

func interpretGrouping(grp golik.Grouping) (string, error) {
	inner, err := interpretCondition(grp.InnerGroup())
	if err != nil {
		return "", err
	}
	return fmt.Sprintln("(", inner, ")"), nil
}