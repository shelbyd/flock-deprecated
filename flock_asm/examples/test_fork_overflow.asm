# Spawns hundreds of tasks in a way that would overflow a recursive executor.

main:
  PUSH 99999
  DUP

  FORK
  BURY 1
  JMP f, $count
  POP
  JOIN 0

  JMP $print_and_halt

count:
  JMP z, $count_done

  PUSH -1
  ADD

  FORK
  JMP f, $count_fork
  JOIN 0
  HALT

count_done:
  POP
  HALT

count_fork:
  POP
  JMP $count

print_and_halt:
  DUMP_DEBUG
  HALT
