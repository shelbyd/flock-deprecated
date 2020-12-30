# Counts from 3 to 0, printing debug info each time.

main:
  PUSH 3

loop:
  PUSH -1
  ADD
  PUSH $halt
  DUMP_DEBUG
  JMP z

  PUSH $loop
  JMP

halt:
