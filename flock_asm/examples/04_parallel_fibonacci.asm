# Calculates fibonacci(N) with parallel recursion.

main:
  ; This number is large enough that the calculation takes a while with a single core.
  ; So we can see the effects of forking.
  PUSH 29

  FORK
  BURY 1
  JMP f, $fibonacci
  POP
  JOIN 1

  JMP $print_and_halt

fibonacci:
  JMP z, $fibonacci_0

  PUSH -1
  ADD

  JMP z, $fibonacci_0

  DUP
  PUSH -1
  ADD

  FORK
  JMP f, $fibonacci_fork
  BURY 2
  POP

  FORK
  JMP f, $fibonacci_fork
  BURY 2
  POP

  JOIN 1
  DREDGE 1
  JOIN 1

  ADD
  HALT

fibonacci_0:
  POP
  PUSH 1
  HALT

fibonacci_fork:
  POP
  JMP $fibonacci

print_and_halt:
  DUMP_DEBUG
  HALT
