pacman::p_load(tidyverse, here)

test_ouptut <- str_c("Test succeeded at ", Sys.time())
# Write the test output into a file
write(test_ouptut, file = here("results/test_output.txt"))