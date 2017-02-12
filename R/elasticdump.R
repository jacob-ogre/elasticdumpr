# BSD_2_clause

#' Perform a basic elasticdump call
#'
#' @details Elasticdump provides basic transfer of elasticsearch indices/types
#' to and from http and file. The \code{src} may be an elasticsearch server
#'
#' @param src The data source; may be http or file (see Details)
#' @param dest The data destination; may be http or file (see Details)
#' @param limit The max. number of docs to dump per batch
#' @param type One of data (default), mapping, or analyzer
#'
#' @return The return code from `elasticdump`; 0 = success
#' @export
#'
elasticdump <- function(src, dest, limit = 100, type = "data") {
  if(grepl(src, pattern = "rm |sudo |delete") |
       grepl(dest, pattern = "rm |sudo |delete")) {
    stop("Suspect pattern in src or dest.")
  }
  cmd <- paste0(
    "elasticdump",
    " --input=", src,
    " --output=", dest,
    " --limit=", limit,
    " --type=", type
  )
  res <- try(system(cmd, intern = FALSE), silent = TRUE)
  if(class(res) == "try-error") {
    stop(
      paste("There was an error:", res)
    )
  }
  return(res)
}