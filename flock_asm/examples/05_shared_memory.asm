; This program spawns many tasks that all check the memory value is as expected.
; Unset memory is 0 by default, so this program will panic if the value is 0.

value = 0x0
task_list_size = 0x1
task_list_start = 0x2

main:
  ; Special value that all tasks check.
  PUSH 42
  STORE $value

  ; Number of tasks to spawn that check the shared memory value.
  PUSH 1000
  DUP
  STORE $task_list_size

spawn_tasks:
  JMP z, $join
  PUSH -1
  ADD

  DUP
  FORK
  JMP f, $check_value

  DREDGE 1
  STORE_REL $task_list_start
  JMP $spawn_tasks

join:
  LOAD $task_list_size
join_loop:
  JMP z, $join_halt
  PUSH -1
  ADD

  DUP
  LOAD_REL $task_list_start
  JOIN 0

  JMP $join_loop

join_halt:
  HALT

check_value:
  LOAD $value
  PUSH -42
  ADD
  JMP z, $value_ok
  PANIC

value_ok:
  HALT
