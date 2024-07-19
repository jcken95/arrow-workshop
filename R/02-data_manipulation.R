library(arrow)
library(dplyr)

seattle_csv <- arrow::open_dataset(
  sources = "data/seattle-library-checkouts.csv",
  format = "csv",
  col_types = arrow::schema(ISBN = arrow::string())
)


seattle_csv |>
  dplyr::mutate(
    is_book = endsWith(MaterialType, "BOOK")
  ) |>
  dplyr::select(MaterialType, is_book)
# generated a query
# print out shows some stuff arrow cpp lib understands
# relies on lazyeval


seattle_csv |>
  utils::head(20) |> # just to preview data
  dplyr::mutate(
    is_book = endsWith(MaterialType, "BOOK")
  ) |>
  dplyr::select(MaterialType, is_book) |>
  dplyr::collect() # run query

# more involved query

seattle_csv |>
  dplyr::filter(endsWith(MaterialType, "BOOK")) |>
  dplyr::group_by(CheckoutYear) |>
  dplyr::summarise(
    checkouts = sum(Checkouts)
  ) |>
  dplyr::arrange(CheckoutYear) |>
  dplyr::collect()

seattle_csv |>
  dplyr::filter(endsWith(MaterialType, "BOOK")) |>
  dplyr::group_by(CheckoutYear) |>
  dplyr::summarise(
    checkouts = sum(Checkouts)
  ) |>
  dplyr::arrange(CheckoutYear) |>
  dplyr::collect() |>
  system.time()
