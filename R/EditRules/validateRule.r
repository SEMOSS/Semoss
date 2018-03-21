validateRule <- function(rule) {

df <- data.frame(
  rule = c(rule)
  , label = c("rule")
)

tryCatch({
		v <- validator(.data=df) 
		if(exists('v')) {
			return("TRUE")
	    }
	  }, error = function(e) {
			return("FALSE")
	})
}
			