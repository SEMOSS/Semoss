# Utility functions that are sourced by RRoutine.java

# Default result function
# The user can override this in his or her analytics script
# RRoutine.java looks for this function,
# so having this avoids an error
GetResult <- function() {
  # Return nothing
}