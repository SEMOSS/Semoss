
getRegex <- function(options, numOptions, rule){
  # email regexes
  regexArray <- character(numOptions)
  if(rule == 'Email Format') {
    for(i in 1:numOptions) {
      form <- options[i]
      if(form == "xxxxx@xxxx.xxx"){regexArray[i] <- ("^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}$")}
      else if(form == "xxxxx@xxxx.xx.xx"){regexArray[i] <- ("^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}\\.[a-zA-Z]{2,3}$")}
      else if(form == "xxxxx@xxxx.xxx(.xx)"){regexArray[i] <- ("^([a-zA-Z0-9_\\-\\.]+)@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\\]?)$")}
    }
  }
  #date regexes
  else if(rule == 'Date Format') {
    for(i in 1:numOptions) {
      form <- options[i]
      if(form == "mm/dd/yyyy"){regexArray[i] <- "^[[:digit:]]{,2}\\/[[:digit:]]{,2}\\/[[:digit:]]{4}$"}
      else if(form == "month dd, yyyy"){regexArray[i] <- ("^(?:J(anuary|u(ne|ly))|February|Ma(rch|y)|A(pril|ugust)|(((Sept|Nov|Dec)em)|Octo)ber)[_]?(0?[1-9]|[1-2][0-9]|3[01]),[_](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
      else if(form == "day, month dd, yyyy"){regexArray[i] <- ("^(?:\\s*(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),[_]*)(?:J(anuary|u(ne|ly))|February|Ma(rch|y)|A(pril|ugust)|(((Sept|Nov|Dec)em)|Octo)ber)[_]?(0?[1-9]|[1-2][0-9]|3[01]),[_](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
      else if(form == "mon dd, yyyy"){regexArray[i] <- ("^(?:J(an|u(n|l))|Feb|Ma(r|y)|A(pr|ug)|Sep|Nov|Dec|Oct)[_]?(0?[1-9]|[1-2][0-9]|3[01]),[_](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
    }
  }
  # name regexes
  else if(rule == 'Name Format') {
    for(i in 1:numOptions) {
      form <- options[i]
      if(form == "last, first (m.)"){regexArray[i] <- ("^\\w{1,12},[_]\\w{1,12}([_]\\w?.)*$")}
      else if(form == "first last"){regexArray[i] <- ("^\\w{1,12}[_]\\w{1,12}$")}
    }
  }
  return (regexArray)
}


# whatDateRule <- function(options, numOptions){
#   regexArray <- character(numOptions)
#   for(i in 1:numOptions){
#     form <- options[i]
#     if(form == "mm/dd/yyyy"){regexArray[i] <- "^[[:digit:]]{,2}\\/[[:digit:]]{,2}\\/[[:digit:]]{4}$"}
#     else if(form == "month dd, yyyy"){regexArray[i] <- ("^(?:J(anuary|u(ne|ly))|February|Ma(rch|y)|A(pril|ugust)|(((Sept|Nov|Dec)em)|Octo)ber)[_]?(0?[1-9]|[1-2][0-9]|3[01]),[_](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
#     else if(form == "day, month dd, yyyy"){regexArray[i] <- ("^(?:\\s*(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),[_]*)(?:J(anuary|u(ne|ly))|February|Ma(rch|y)|A(pril|ugust)|(((Sept|Nov|Dec)em)|Octo)ber)[_]?(0?[1-9]|[1-2][0-9]|3[01]),[_](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
#     else if(form == "mon dd, yyyy"){regexArray[i] <- ("^(?:J(an|u(n|l))|Feb|Ma(r|y)|A(pr|ug)|Sep|Nov|Dec|Oct)[_]?(0?[1-9]|[1-2][0-9]|3[01]),[_](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
#   }
#   return (regexArray)
# }
# 
# whatEmailRule <- function(form){
#   regexArray <- character(numOptions)
#   for(i in 1:numOptions){
#     form <- options[i]
#     if(form == "xxxxx@xxxx.xxx"){regexArray[i] <- ("^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}$")}
#     else if(form == "xxxxx@xxxx.xx.xx"){regexArray[i] <- ("^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}\\.[a-zA-Z]{2,3}$")}
#     else if(form == "xxxxx@xxxx.xxx(.xx)"){regexArray[i] <- ("^([a-zA-Z0-9_\\-\\.]+)@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\\]?)$")}
#   }
#   return (regexArray)
# }

