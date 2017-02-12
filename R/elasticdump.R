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
elasticdump <- function(src, dest, limit = 100, type = "data", rm_dest = TRUE) {
  if(grepl(src, pattern = "rm |sudo |delete") |
       grepl(dest, pattern = "rm |sudo |delete")) {
    stop("Suspect pattern in src or dest.")
  }
  if(!grepl(src, pattern = "http")) {
    src <- path.expand(src)
  }
  if(!grepl(dest, pattern = "http")) {
    dest <- path.expand(dest)
    if(file.exists(dest) & rm_dest) {
      file.remove(dest)
    } else if(file.exists(dest) & !rm_dest) {
      return("Destination file exists.")
    }
  }
  cmd <- paste0(
    "elasticdump",
    " --input=", src,
    " --output=", dest,
    " --limit=", limit,
    " --type=", type
  )
  res <- suppressMessages(try(
    system(cmd, intern = TRUE, wait = TRUE), silent = TRUE))
  if(class(res) == "try-error") {
    message(res)
    return( paste("There was an error:", res) )
  }
  return(res)
}

#' Back up the data from an elasticsearch index + type
#'
#' @param type Elasticsearch type to retrieve
#' @param server Http/s elasticsearch server address
#' @param index Name of the elasticsearch index to retrieve
#' @param bak_dir Path of directory to which the backup is written
#'
#' @return The return code from `elasticdump`; 0 == success
#' @export
#'
es_data_backup <- function(type, server, index, bak_dir) {
  src = file.path(server, index, type)
  dest = file.path(
    path.expand(bak_dir),
    paste0("bak_", index, "_", type, "_data_", Sys.Date(), ".json")
  )
  result <- elasticdump(src = src, dest = dest)
  return(result)
}

#' Back up the analyzer of an elasticsearch index
#'
#' @param server Http/s elasticsearch server address
#' @param index Name of the elasticsearch index to retrieve
#' @param bak_dir Path of directory to which the backup is written
#'
#' @return The return code from `elasticdump` (0 == success) or FALSE if
#'   \code{server} isn't an http/s address.
#' @export
#'
es_analyzer_backup <- function(server, index, bak_dir) {
  if(!grepl(server, pattern = "http://|https://")) {
    message("The analyzer backup source should be an http/s address.")
    return(FALSE)
  }
  src = file.path(server, index)
  dest = file.path(
    path.expand(bak_dir),
    paste0("bak_", index, "_analyzer_", Sys.Date(), ".json")
  )
  result <- elasticdump(src = src, dest = dest, type = "analyzer")
  return(result)
}

#' Back up the mapper of an elasticsearch index + type
#'
#' @param server Http/s elasticsearch server address
#' @param index Name of the elasticsearch index to retrieve
#' @param type Elasticsearch type to retrieve
#' @param bak_dir Path of directory to which the backup is written
#'
#' @return The return code from `elasticdump` (0 == success) or FALSE if
#'   \code{server} isn't an http/s address.
#' @export
#'
es_mapping_backup <- function(server, index, type, bak_dir) {
  if(!grepl(server, pattern = "http://|https://")) {
    message("The mapping backup source should be an http/s address.")
    return(FALSE)
  }
  src = file.path(server, index, type)
  dest = file.path(
    path.expand(bak_dir),
    paste0("bak_", index, "_", type, "_mapping_", Sys.Date(), ".json")
  )
  result <- elasticdump(src = src, dest = dest, type = "mapping")
  return(result)
}