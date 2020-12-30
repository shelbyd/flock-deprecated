# Calculates fibonacci(10) with recursion.

main:
  PUSH 10
  JSR $fibonacci

  JMP $halt

fibonacci:
  BURY 1
  JMP z, $fibonacci_0

  PUSH -1
  ADD

  JMP z, $fibonacci_0

  DUP
  PUSH -1
  ADD

  JSR $fibonacci
  BURY 1

  JSR $fibonacci

  ADD
  DREDGE 1
  RET

fibonacci_0:
  POP
  PUSH 1
  DREDGE 1
  RET

halt:
  DUMP_DEBUG
