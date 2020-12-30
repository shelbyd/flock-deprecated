# Calculates fibonacci(10) with recursion.

main:
  PUSH 10
  PUSH $fibonacci
  JSR

  PUSH $halt
  JMP

fibonacci:
  BURY 1
  PUSH $fibonacci_0
  JMP z

  PUSH -1
  ADD
  DUP

  PUSH $fibonacci
  JSR
  BURY 1

  PUSH -1
  ADD
  PUSH $fibonacci
  JSR

  ADD
  RET

fibonacci_0:
  POP
  PUSH 1
  DREDGE 1
  RET

halt:
  DUMP_DEBUG
