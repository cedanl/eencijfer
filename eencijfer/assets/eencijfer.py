"""Eencijfer data asset."""

import logging
import colorlog
from pathlib import Path

import pandas as pd

from eencijfer.assets.transformations.diploma import _add_ho_diploma_eerstejaar, _add_soort_diploma
from eencijfer.assets.transformations.opleiding import (
    _add_croho_onderdeel,
    _add_isced,
    _add_lokale_naam_opleiding_faculteit,
    _add_naam_opleiding,
    _add_opleiding,
    _add_type_opleiding,
)
from eencijfer.assets.transformations.prestatieafspraken import _add_pa_cohort
from eencijfer.assets.transformations.vooropleiding import _add_naam_instelling_vooropleiding, _add_vooropleiding
from eencijfer.utils.detect_eencijfer_files import _get_eencijfer_datafile

# Configure colorlog
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    "%(log_color)s%(levelname)s:%(name)s:%(message)s",
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
))

logger = colorlog.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

def _create_eencijfer_df(source_dir: Path):
    """Pipeline voor verrijken van eencijfer-basisbestand."""

    eencijfer_fname = _get_eencijfer_datafile(source_dir)
    if eencijfer_fname:
        eencijfer = pd.read_parquet(Path(source_dir / eencijfer_fname).with_suffix('.parquet'))

    if not isinstance(eencijfer, pd.DataFrame):
        raise Exception(f'No data found {eencijfer_fname}')

    eencijfer["Aantal"] = 1

    # voeg informatie over vooropleiding toe:
    eencijfer = _add_vooropleiding(eencijfer, vooropleiding_field="HoogsteVooropleiding")

    eencijfer = _add_naam_instelling_vooropleiding(
        eencijfer,
        vooropleiding="HoogsteVooropleiding",
    )
    # voeg informatie over inschrijving toe:
    try:
        eencijfer = _add_naam_opleiding(eencijfer)
    except Exception as e:
        logger.error(f"Failed to add naam opleiding: {e}")

    try:
        eencijfer = _add_opleiding(eencijfer)
    except Exception as e:
        logger.error(f"Failed to add opleiding: {e}")

    try:
        eencijfer = _add_croho_onderdeel(eencijfer)
    except Exception as e:
        logger.error(f"Failed to add croho onderdeel: {e}")

    try:
        eencijfer = _add_soort_diploma(eencijfer)
    except Exception as e:
        logger.error(f"Failed to add soort diploma: {e}")

    try:
        eencijfer = _add_ho_diploma_eerstejaar(eencijfer)
    except Exception as e:
        logger.error(f"Failed to add ho diploma eerstejaar: {e}")

    try:
        eencijfer = _add_type_opleiding(eencijfer)
    except Exception as e:
        logger.error(f"Failed to add type opleiding: {e}")

    try:
        eencijfer = _add_lokale_naam_opleiding_faculteit(eencijfer)
    except Exception as e:
        logger.error(f"Failed to add lokale naam opleiding faculteit: {e}")

    try:
        eencijfer = _add_pa_cohort(eencijfer)
    except Exception as e:
        logger.error(f"Failed to add pa cohort: {e}")

    try:
        eencijfer = _add_isced(eencijfer)
    except Exception as e:
        logger.error(f"Failed to add isced: {e}")

    logger.critical(f"Columns in eencijfer: {eencijfer.columns}")

    return eencijfer
