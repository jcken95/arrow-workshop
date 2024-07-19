dir.create("data")

#  full

withr::with_options(
  list(timeout = 2000),
  download.file(
    url = "https://r4ds.s3.us-west-2.amazonaws.com/seattle-library-checkouts.csv",
    destfile = "data/seattle-library-checkouts.csv"
  )
)


#  lite

download.file(
  url = "https://github.com/posit-conf-2023/arrow/releases/download/v0.1.0/seattle-library-checkouts-tiny.csv",
  destfile = "./data/seattle-library-checkouts-tiny.csv"
)
