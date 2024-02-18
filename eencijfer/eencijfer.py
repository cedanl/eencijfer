"""Main eencijfer module."""
import configparser
import logging
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import pandas as pd
import typer

from eencijfer import CONVERTERS
from eencijfer.settings import config

logger = logging.getLogger(__name__)


class ExportFormat(str, Enum):
    """File format that will be used to convert to.

    Args:
        str (_type_): _description_
        Enum (_type_): _description_
    """

    csv = "csv"
    parquet = "parquet"
    db = "db"


def _match_file_to_definition(fpath: Path, config: configparser.ConfigParser = config) -> Optional[Path]:
    """Matches import-definitions to .asc-files in eencijfer-directory.

    Args:
        fpath (Path): Path to .asc-file.
        config (configparser.ConfigParser, optional): conf-object. Defaults to config.

    Returns:
        Optional[Path]: Path to definition file.
    """
    definition_dir = config.get('default', 'import_definitions_dir')
    logger.debug(f"maak lijst met bestanden in {definition_dir}")
    definition_files = [p for p in Path(definition_dir).iterdir() if p.suffix in [".csv"]]
    logger.debug(f"...{len(definition_files)} bestanden gevonden in {definition_dir}")

    # check for duplicate definion files.
    if len(set(definition_files)) != len(definition_files):
        raise Exception('Duplicate definition files found.')

    matching_definition_file = None

    try:
        matching_definition_file = [d for d in definition_files if fpath.stem == d.stem][0]
    except TypeError:
        logger.debug(f"No definition file found for {fpath.stem} yet...")
    except IndexError:
        pass

    if not matching_definition_file and 'EV' in fpath.stem:
        try:
            matching_definition_file = [d for d in definition_files if d.stem.startswith('EV')][0]
        except TypeError:
            pass
        except IndexError:
            pass
    if not matching_definition_file and 'VAKHAV' in fpath.stem:
        try:
            matching_definition_file = [d for d in definition_files if d.stem.startswith('VAKHAV')][0]
        except TypeError:
            pass
        except IndexError:
            pass

    logger.debug(f"Definition-file for {fpath.stem} is set to: {matching_definition_file}")
    return matching_definition_file


def _get_list_of_eencijfer_files_in_dir(config: configparser.ConfigParser = config) -> Optional[list]:
    """Gets list of eencijfer-files that are in eencijfer_dir.

    Get a list of eencijfer-files in the given source_dir (in config-file). Gives a list
    of files that match the names of the files in the definition-dir.

    Args:
        config (configparser.ConfigParser, optional): _description_. Defaults to config.

    Returns:
        Optional[list]: List of files that are recognized as eencijfer-files.
    """
    source_dir = config.get('default', 'source_dir')
    files = None
    try:
        logger.debug("Getting list of files ending with '.asc'...")
        asc_files = [p for p in Path(source_dir).iterdir() if p.suffix.lower() == '.asc']
        logger.debug("...looking for file starting with 'EV'...")
        files = asc_files + [p for p in Path(source_dir).iterdir() if p.stem.startswith('EV')]
        if len(files) == 0:
            typer.echo(f"No files found that in {source_dir} that could be eencijfer-files. Aborting...")
            raise typer.Exit()
        else:
            typer.echo(f"Found {len(files)} files in `{source_dir}` that might be eencijfer-files.")
    except FileNotFoundError:
        typer.echo(f'No files found. Does the directory `{source_dir}` exist?')
        raise typer.Exit()

    return files


def _create_dict_matching_eencijfer_and_definition_files() -> dict:
    """Creates dictionary with matching eencijfer and definition-files.

    Returns:
        dict: Dictionary with eencijfer-definition-file pairs.
    """
    eencijfer_files = _get_list_of_eencijfer_files_in_dir()
    result_dict = {}
    if eencijfer_files is None:
        raise Exception('No files found!')

    for eencijfer_file in eencijfer_files:
        matching_definition_file = _match_file_to_definition(eencijfer_file)
        if isinstance(matching_definition_file, Path):
            result_dict[eencijfer_file] = matching_definition_file
            logger.debug(f"{eencijfer_file.stem} is matched to: {matching_definition_file}")

    return result_dict


def _create_definition_with_converter(
    definition_file: Path,
) -> pd.DataFrame:
    """Creates a dataframe with metadata based an the definition-file.

    Adds a column to the data from a definition-file with a convert-function,
    if no converter is given the column will be in string format.

    Args:
        definition_file (Path): Path to definition-file.

    Returns:
        pd.DataFrame: Definitionfile with converterfunction
    """
    # maak een dataframe met converter-convertfunction mapping
    if not definition_file.exists():
        raise Exception(f"{definition_file} does not exist.")

    logger.debug("Create df with column_converter functions from CONVERTERS-dict.")
    converter_functions = (
        pd.DataFrame.from_dict(
            CONVERTERS,
            orient="index",
        )
        .reset_index()
        .rename(columns={"index": "Converter", 0: "ConvertFunction"})
    )
    logger.debug(f"{converter_functions}")
    logger.debug(f"...reading definition file {definition_file}")
    definition = pd.read_csv(definition_file)
    logger.debug(f"...add column with column_converter_functions to definition {definition_file.name}")

    definition = pd.merge(
        definition,
        converter_functions,
        left_on="Converter",
        right_on="Converter",
        how="left",
    )
    logger.debug(f"{definition}")

    missing_converters = definition[definition.ConvertFunction.isnull()].Label.tolist()

    if len(missing_converters) > 0:
        logger.warning(f"====> missing converters voor: {missing_converters}")
        logger.warning("====> setting converters to 'convert_to_string'")
        definition["ConvertFunction"] = definition["ConvertFunction"].fillna(CONVERTERS['convert_to_object'])
    return definition


def read_asc(fpath: Path, definition_file: Path, use_column_converters: bool = False) -> pd.DataFrame:
    """Reads in asc-file based on definition-file.

    Converters contain column-names, widths and column-converters which are used for
    converting data to the right datatype. For example: convert strings to int64 or
    set a '0-value' as a missing (NaN).

    Args:
        fpath (Path): Path to asc-file.
        definition_file (Path): Path to definition-file.
        use_column_converters (Boolean): wether to use column_converters defined in the definition-file or not.

    Returns:
        pd.DataFrame: df with data from asc-file.
    """

    definition = _create_definition_with_converter(definition_file)

    widths = definition["NumberOfPositions"].tolist()
    names = definition["Label"].tolist()
    column_converters = pd.Series(definition.ConvertFunction.values, index=definition.Label).to_dict()
    logger.info(f"...start reading {fpath.name}")

    try:
        # if column_converters should be used, use them...
        if use_column_converters:
            logger.info(f"...using column converters for {fpath.name}")
            data = pd.read_fwf(
                fpath,
                widths=widths,
                names=names,
                converters=column_converters,
                encoding="latin1",
            )
        else:
            logger.info(f"...import all columns as strings from {fpath.name}")
            data = pd.read_fwf(
                fpath,
                widths=widths,
                names=names,
                dtype='str',
                encoding="latin1",
            )

        if len(data) == 0:
            logger.info(f"...no data found in {fpath.name}")
        else:
            logger.info(f"...data was read from {fpath.name}")

    except Exception as e:
        logger.warning(f"...inlezen van {fpath.name} mislukt.")
        logger.warning(f"{e}")

    return data


def _save_to_file(
    df: pd.DataFrame,
    fname: str = "unknown",
    export_format: str = 'parquet',
    config: configparser.ConfigParser = config,
):
    """Saves data in the export_format in the result-directory specified in the config.

    Args:
        df (pd.DataFrame): _description_
        fname (str, optional): _description_. Defaults to "unknown".
        export_format (ExportFormat, optional): _description_. Defaults to ExportFormat.parquet.
        config (configparser.ConfigParser, optional): _description_. Defaults to config.

    Returns:
        None: None
    """

    today = datetime.today().strftime("%Y-%m-%d")
    # voeg datum-directory toe aan path en maak aan:
    result_dir = config.get('default', 'result_dir')
    result_dir_date = Path(result_dir) / today

    Path(result_dir_date).mkdir(parents=True, exist_ok=True)

    # logger.debug(f"Renaming according to naming_convention: {naming_convention}...")

    # if naming_convention == "pascalcase":
    #     name = pascalcase(name)
    #     new_col_names = [pascalcase(col) for col in data.columns]
    #     data.columns = new_col_names
    # elif naming_convention == "snakecase":
    #     name = snakecase(name)
    #     new_col_names = [snakecase(col) for col in data.columns]
    #     data.columns = new_col_names

    fsuffix = "." + export_format
    target_fpath = Path(result_dir_date / fname).with_suffix(fsuffix)
    logger.info(f"Saving {fname} to {target_fpath}...")
    if export_format == 'csv':
        df.to_csv(target_fpath, sep=",")
    if export_format == 'parquet':
        df.to_parquet(target_fpath)

    return None


def _convert_to_export_format(
    config: configparser.ConfigParser = config,
    export_format: str = 'parquet',
    use_column_converters: bool = False,
) -> None:
    """Saves data to the export format.

    Main function that reads and converts to export-format, to the
    result-directory specified in the config-file.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        fname (str, optional): The filename to use for saving. Defaults to "unknown".
        export_format (str, optional): The export format to use. Defaults to 'parquet'.
        config (configparser.ConfigParser, optional): The configuration parser
          containing the result directory. Defaults to config.

    Returns:
        None: This function does not return a value.
    """
    # get dict with files and definitions:
    eencijfer_definition_pairs = _create_dict_matching_eencijfer_and_definition_files()
    result_dir = Path(config.get('default', 'result_dir'))

    for file, definition_file in eencijfer_definition_pairs.items():
        target_fpath = Path(result_dir / file.name).with_suffix(".parquet")

        logger.info("**************************************")
        logger.info("**************************************")
        logger.info("")
        logger.info(f"   Start reading: {file.name}")
        logger.info("")
        logger.info(f"   source_file:{file}")
        logger.info(f"   definition_file:{definition_file}")
        logger.info(f"   target_fpath:{target_fpath}")
        logger.info("")
        logger.info("**************************************")
        logger.info("")

        try:
            raw_data = read_asc(file, definition_file, use_column_converters=use_column_converters)

            if len(raw_data) > 0:
                logger.warning(f"...reading {file.name} succeeded.")
                _save_to_file(raw_data, fname=file.stem, export_format=export_format, config=config)

            else:
                logger.info(f"...there does not seem to be data in {file.name}!")
        except Exception as e:
            logger.warning(f"...inlezen van {file.name} mislukt.")
            logger.warning(f"{e}")

        logger.info("**************************************")
    return None
