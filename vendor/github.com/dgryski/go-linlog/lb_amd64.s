// +build amd64,!appengine

// func lb(x uint64) uint64
TEXT ·lb(SB), 4, $0-16
	BSRQ x+0(FP), AX
	JZ   zero
	MOVQ AX, ret+8(FP)
	RET

zero:
	MOVQ $64, ret+8(FP)
	RET
