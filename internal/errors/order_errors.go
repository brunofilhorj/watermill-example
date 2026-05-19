package errors

import "errors"

var (
	ErrSaldoInsuficiente   = errors.New("saldo insuficiente")
	ErrEstoqueIndisponivel = errors.New("estoque indisponível")
	ErrValorInvalido       = errors.New("valor inválido")
)
