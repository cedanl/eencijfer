"""Data asset cohorten."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from eencijfer.assets.eencijfer import _create_eencijfer_df
from eencijfer.settings import config

logger = logging.getLogger(__name__)

def create_actief_hoofd_eerstejaar_instelling(source_dir: Path, eencijfer: pd.DataFrame) -> pd.DataFrame:
    """Create df with actieve hoofdinschrijving, eerste jaar instelling.

    Every student occurs only once every year (cohort year). Only
    inschrijvingen active at oktober 1. are present.

    Returns:
        pd.DataFrame: Dataframe.
    """
    print(f"DEBUG: eencijfer DataFrame shape: {eencijfer.shape}")
    print(f"DEBUG: eencijfer columns: {eencijfer.columns.tolist()}")

    # Actief op 1 oktober
    filter_actiefopPeildatum = eencijfer.IndicatieActiefOpPeildatum == 1
    print(f"DEBUG: Rows with IndicatieActiefOpPeildatum == 1: {filter_actiefopPeildatum.sum()}")

    print(f"DEBUG: Unique values in SoortInschrijvingHogerOnderwijs: {eencijfer['SoortInschrijvingHogerOnderwijs'].unique()}")
    print(f"DEBUG: Value counts of SoortInschrijvingHogerOnderwijs:\n{eencijfer['SoortInschrijvingHogerOnderwijs'].value_counts()}")


    # Hoofdinschrijving
    filter_soortinschrijving_ho = eencijfer.SoortInschrijvingHogerOnderwijs == '1'
    print(f"DEBUG: Rows with SoortInschrijvingHogerOnderwijs == 1: {filter_soortinschrijving_ho.sum()}")

    # Eerstejaar
    filter_eerstejaar_instelling = eencijfer.Inschrijvingsjaar == eencijfer.EersteJaarAanDezeActueleInstelling
    print(f"DEBUG: Rows with Inschrijvingsjaar == EersteJaarAanDezeActueleInstelling: {filter_eerstejaar_instelling.sum()}")

    instroom = eencijfer[
        (filter_eerstejaar_instelling) & (filter_soortinschrijving_ho) & (filter_actiefopPeildatum)
    ].copy()

    print(f"DEBUG: Final instroom DataFrame shape: {instroom.shape}")

    return instroom


# def create_actief_hoofd_eerstejaar_instelling(source_dir: Path, eencijfer: pd.DataFrame) -> pd.DataFrame:
#     """Create df with actieve hoofdinschrijving, eerste jaar instelling.

#     Every student occurs only once every year (cohort year). Only
#     inschrijvingen active at oktober 1. are present.

#     Returns:
#         pd.DataFrame: Dataframe.
#     """
#     # Actief op 1 oktober
#     # eencijfer = _create_eencijfer_df(source_dir)
#     filter_actiefopPeildatum = eencijfer.IndicatieActiefOpPeildatum == 1
#     # Hoofdinschrijving
#     filter_soortinschrijving_ho = eencijfer.SoortInschrijvingHogerOnderwijs == 1
#     # Eerstejaar
#     filter_eerstejaar_instelling = eencijfer.Inschrijvingsjaar == eencijfer.EersteJaarAanDezeActueleInstelling

#     instroom = eencijfer[
#         (filter_eerstejaar_instelling) & (filter_soortinschrijving_ho) & (filter_actiefopPeildatum)
#     ].copy()
#     return instroom


def create_inschrijving_jaar2(source_dir: Path, eencijfer: pd.DataFrame) -> pd.DataFrame:
    """Filters eencijfer for second year institution with hoofdopleiding.

    Returns:
        pd.DataFrame: Df with hoofdinschrijvingen in year 2.
    """
    eencijfer = _create_eencijfer_df(source_dir)

    filter_tweede_jaar = eencijfer.Inschrijvingsjaar == eencijfer.EersteJaarAanDezeActueleInstelling + 1
    filter_soortinschrijving_ho = eencijfer.SoortInschrijvingHogerOnderwijs == 1

    fields_jaar2 = [
        "PersoonsgebondenNummer",
        "opleiding",
        "ActueleInstelling",
        "Opleidingsvorm",
        "OpleidingActueelEquivalent",
    ]

    inschrijvingen_tweede_jaar = eencijfer[(filter_tweede_jaar) & (filter_soortinschrijving_ho)][fields_jaar2].copy()
    return inschrijvingen_tweede_jaar

def create_propedeuse_diplomas(eencijfer: pd.DataFrame) -> pd.DataFrame:
    """Create a table with propedeuse-diplomas.

    Returns:
        pd.DataFrame: _description_
    """

    source_dir = config.getpath('default', 'source_dir')
    eencijfer = _create_eencijfer_df(source_dir)
    print(f"DEBUG: eencijfer shape at start: {eencijfer.shape}")

    fields = [
        "PersoonsgebondenNummer",
        "OpleidingActueelEquivalent",
        "EersteJaarAanDezeActueleInstelling",
        "DatumTekeningDiploma",
        "opleiding",
        "Diplomajaar",
    ]

    print(f"DEBUG: Columns in eencijfer: {eencijfer.columns.tolist()}")

    # Check if required fields are present
    missing_fields = [field for field in fields if field not in eencijfer.columns]
    if missing_fields:
        print(f"DEBUG: Missing fields in eencijfer: {missing_fields}")

    print(eencijfer['OpleidingsfaseActueelVanHetDiploma'].unique())


    # Alleen het eerst gehaalde propedeuse-diploma telt mee.
    filter_diplomajaar = eencijfer.Diplomajaar.notnull()
    filter_opleidingsfase = eencijfer.OpleidingsfaseActueelVanHetDiploma == "D"

    print(f"DEBUG: Rows with non-null Diplomajaar: {filter_diplomajaar.sum()}")
    print(f"DEBUG: Rows with OpleidingsfaseActueelVanHetDiploma == 'D': {filter_opleidingsfase.sum()}")

    propedeusediplomas = eencijfer[filter_diplomajaar & filter_opleidingsfase][fields]
    print(f"DEBUG: Shape after initial filtering: {propedeusediplomas.shape}")

    propedeusediplomas = (
        propedeusediplomas
        .sort_values(by="Diplomajaar", ascending=True)
        .drop_duplicates(
            subset=[
                "PersoonsgebondenNummer",
            ],
            keep="first",
        )
        .copy()
    )
    print(f"DEBUG: Shape after sorting and dropping duplicates: {propedeusediplomas.shape}")

    propedeusediplomas = propedeusediplomas.rename(
        columns={
            "Diplomajaar": "JaarPropedeuseDiploma",
        }
    )

    print(f"DEBUG: Final shape of propedeusediplomas: {propedeusediplomas.shape}")

    return propedeusediplomas

# def create_propedeuse_diplomas(eencijfer: pd.DataFrame) -> pd.DataFrame:
#     """Create a table with propedeuse-diplomas.

#     Returns:
#         pd.DataFrame: _description_
#     """
#     # source_dir = config.getpath('default', 'source_dir')
#     # eencijfer = _create_eencijfer_df(source_dir)

#     fields = [
#         "PersoonsgebondenNummer",
#         "OpleidingActueelEquivalent",
#         "EersteJaarAanDezeActueleInstelling",
#         "DatumTekeningDiploma",
#         "opleiding",
#         "Diplomajaar",
#     ]

#     # Alleen het eerst gehaalde propedeuse-diploma telt mee.
#     propedeusediplomas = (
#         eencijfer[(eencijfer.Diplomajaar.notnull()) & (eencijfer.OpleidingsfaseActueelVanHetDiploma == "D")][fields]
#         .sort_values(by="Diplomajaar", ascending=True)
#         .drop_duplicates(
#             subset=[
#                 "PersoonsgebondenNummer",
#             ],
#             keep="first",
#         )
#         .copy()
#     )
#     propedeusediplomas = propedeusediplomas.rename(
#         columns={
#             "Diplomajaar": "JaarPropedeuseDiploma",
#         }
#     )

#     return propedeusediplomas


def merge_cohort_inschrijving_jaar2(instroom: pd.DataFrame, inschrijvingen_tweede_jaar: pd.DataFrame) -> pd.DataFrame:
    """Merge of instroom with inschrijving jaar 2.

    Args:
        instroom (pd.DataFrame): instroom table.
        inschrijvingen_tweede_jaar (pd.DataFrame): tweede jaar inschrijving.

    Returns:
        pd.DataFrame: _description_
    """
    instroom_uitval_switch = pd.merge(
        instroom,
        inschrijvingen_tweede_jaar,
        left_on="PersoonsgebondenNummer",
        right_on="PersoonsgebondenNummer",
        how="left",
        suffixes=["", "_2ejaar"],
    )

    if not len(instroom) == len(instroom_uitval_switch):
        raise Exception("Merging gave too much rows.")

    return instroom_uitval_switch


def create_bachelordiplomas(eencijfer: pd.DataFrame) -> pd.DataFrame:
    """Create table with bachelor-degrees.

    Args:
        eencijfer (pd.DataFrame): Eencijfer-table.

    Returns:
        pd.DataFrame: Df with bachelor-degrees.
    """

    fields = [
        "PersoonsgebondenNummer",
        "OpleidingActueelEquivalent",
        "opleiding",
        "DatumTekeningDiploma",
        "Diplomajaar",
        "Opleidingsvorm",
        "Aantal",
    ]

    bachelordiplomas = eencijfer[(eencijfer.Diplomajaar != 0) & (eencijfer.OpleidingsfaseActueelVanHetDiploma == "B")][
        fields
    ].copy()
    return bachelordiplomas


def add_propedeuse_in_1_jaar(data: pd.DataFrame, eencijfer: pd.DataFrame) -> pd.DataFrame:
    """Add 1/0 column with propedeuse in 1 year and Uitval1JaarMetPropedeuse.

    Args:
        data (pd.DataFrame): Eencijfer-df.

    Returns:
        pd.DataFrame: Eencijfer with 2 columns added.
    """

    # p_diploma = data.OpleidingsfaseActueelVanHetDiploma=='D'
    p_diplomas = create_propedeuse_diplomas(eencijfer)
    pgn_p_in_cohort_jaar = p_diplomas[
        p_diplomas.EersteJaarAanDezeActueleInstelling == p_diplomas.JaarPropedeuseDiploma
    ].PersoonsgebondenNummer.tolist()
    pgn_p_in_cohort_jaar_plus_1 = p_diplomas[
        p_diplomas.EersteJaarAanDezeActueleInstelling + 1 == p_diplomas.JaarPropedeuseDiploma
    ].PersoonsgebondenNummer.tolist()
    #    p_in_cohort_jaar_plus_1 = data.EersteJaarAanDezeActueleInstelling + 1 == p_diploma.Diplomajaar

    data["PropedeuseIn1Jaar"] = np.where(data.PersoonsgebondenNummer.isin(pgn_p_in_cohort_jaar), 1, 0)

    data["PropedeuseIn2Jaar"] = np.where(data.PersoonsgebondenNummer.isin(pgn_p_in_cohort_jaar), 1, 0)
    data["PropedeuseIn2Jaar"] = np.where(
        data.PersoonsgebondenNummer.isin(pgn_p_in_cohort_jaar_plus_1),
        1,
        data["PropedeuseIn2Jaar"],
    )

    data["Uitval1JaarMetPropedeuse"] = np.where((data.UitvalEerstejaar == 1) & (data.PropedeuseIn1Jaar == 1), 1, 0)

    return data


def _add_herinschrijver_met_propedeuse(data: pd.DataFrame) -> pd.DataFrame:
    """Add 1/0 column with herinschrijver with propedeuse.

    Args:
        data (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """

    logger.info("Voeg herinschrijving met p toe")

    filter_herinschrijver = data.HerinschrijvingInstelling == 1
    filter_propedeuse = data.PropedeuseIn1Jaar == 1
    data["HerinschrijvingMetPropedeuse"] = np.where((filter_herinschrijver) & (filter_propedeuse), 1, 0)
    return data


def _add_een_diploma(data: pd.DataFrame, bachelordiplomas: pd.DataFrame) -> pd.DataFrame:
    """Add column with 'een bachelordiploma'.

    Args:
        data (pd.DataFrame): _description_
        bachelordiplomas (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: Eencijfer or cohort added columns.
    """
    een_bachelordiploma = (
        bachelordiplomas.sort_values(by="Diplomajaar", ascending=True)
        .drop_duplicates(
            subset=[
                "PersoonsgebondenNummer",
            ],
            keep="first",
        )
        .copy()
    )
    result = pd.merge(
        data,
        een_bachelordiploma,
        left_on="PersoonsgebondenNummer",
        right_on="PersoonsgebondenNummer",
        how="left",
        suffixes=["", "_EenDiploma"],
    )
    result["BachelorDiploma"] = result["Aantal_EenDiploma"].fillna(0)
    result["JaarTotEenDiploma"] = result.Diplomajaar_EenDiploma - result.EersteJaarAanDezeActueleInstelling
    result["EenDiplomaBinnen4jaar"] = np.where(result["JaarTotEenDiploma"] <= 3, 1, 0)
    result["EenDiplomaBinnen5jaar"] = np.where(result["JaarTotEenDiploma"] <= 4, 1, 0)
    result["EenDiplomaBinnen6jaar"] = np.where(result["JaarTotEenDiploma"] <= 5, 1, 0)
    result["EenDiplomaBinnen7jaar"] = np.where(result["JaarTotEenDiploma"] <= 6, 1, 0)
    result["EenDiplomaBinnen8jaar"] = np.where(result["JaarTotEenDiploma"] <= 7, 1, 0)

    if not len(data) == len(result):
        raise Exception('Something went wrong with merge.')

    return result


def _add_dit_diploma(data: pd.DataFrame, bachelordiplomas: pd.DataFrame) -> pd.DataFrame:
    """Add column with 'dit diploma', indicating same croho as in first year.

    Args:
        data (pd.DataFrame): _description_
        bachelordiplomas (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """

    dit_bachelordiploma = (
        bachelordiplomas.sort_values(by="Diplomajaar", ascending=True)
        .drop_duplicates(subset=["PersoonsgebondenNummer", "opleiding"], keep="first")
        .copy()
    )
    result = pd.merge(
        data,
        dit_bachelordiploma,
        left_on=["PersoonsgebondenNummer", "opleiding"],
        right_on=["PersoonsgebondenNummer", "opleiding"],
        how="left",
        suffixes=["", "_DitDiploma"],
    )
    if not len(data) == len(result):
        raise Exception('Something went wrong with merge.')

    return result


def _add_status_student(data: pd.DataFrame, field: str = "StatusNa1Jaar") -> pd.DataFrame:
    """Add StatusNa1Jaar.

    Args:
        data (pd.DataFrame): _description_
        field (str, optional): _description_. Defaults to "StatusNa1Jaar".

    Returns:
        pd.DataFrame: _description_
    """
    data[field] = np.nan
    data[field] = np.where(data["UitvalEerstejaar"] == 1, 1, data[field])
    data[field] = np.where(data["SwitchBinnenInstelling"] == 1, 6, data[field])
    data[field] = np.where(data["HoDiplomaInEersteJaar"] == 1, 4, data[field])

    data[field] = data[field].replace(
        {
            1: "ValtUitInJaar1",
            3: "DiplomaBehaald",
            4: "HoDiplomaInEersteJaar",
            6: "SwitchBinnenInstelling",
        }
    )
    data[field] = data[field].fillna("StudeertNogAanHsl")
    return data


# def create_cohorten_met_indicatoren(source_dir: Path, eencijfer: pd.DataFrame) -> pd.DataFrame:
#     """Create cohorten-table from all parts.

#     Returns:
#         pd.DataFrame: _description_
#     """

#     """Levert een cohortbestand met indicatoren eerste jaar."""
#     # eencijfer = _create_eencijfer_df(source_dir)
#     instroom = create_actief_hoofd_eerstejaar_instelling(source_dir, eencijfer)
#     inschrijvingen_tweede_jaar = create_inschrijving_jaar2(source_dir, eencijfer)
#     propedeusediplomas = create_propedeuse_diplomas(eencijfer)
#     bachelordiplomas = create_bachelordiplomas(eencijfer)

#     result = merge_cohort_inschrijving_jaar2(instroom, inschrijvingen_tweede_jaar)

#     result["UitvalEerstejaar"] = np.where(
#         ((result.ActueleInstelling == result.ActueleInstelling_2ejaar) | (result.HoDiplomaInEersteJaar == 1)),
#         0,
#         1,
#     )
#     # uitval in jaar 1
#     result["opleiding_gelijk"] = np.where(result.opleiding == result.opleiding_2ejaar, 1, 0)
#     result["opleiding_gelijk"] = np.where(
#         result.OpleidingActueelEquivalent == result.OpleidingActueelEquivalent_2ejaar,
#         1,
#         result.opleiding_gelijk,
#     )
#     result["HerinschrijvingInstelling"] = np.where(
#         ((result.opleiding_gelijk == 1) & (result.HoDiplomaInEersteJaar == 0)), 1, 0
#     )
#     result["HerinschrijvingInstelling"] = np.where((result.ActueleInstelling == result.ActueleInstelling_2ejaar), 1, 0)

#     result["SwitchBinnenInstelling"] = np.where(
#         ((result.opleiding_gelijk == 0) & (result.UitvalEerstejaar == 0) & (result.HoDiplomaInEersteJaar == 0)),
#         1,
#         0,
#     )

#     if not len(instroom) == len(result):
#         raise Exception('Something went wrong with merge.')

#     # diploma in 1 jaar:
#     result = add_propedeuse_in_1_jaar(result, eencijfer)

#     result = _add_herinschrijver_met_propedeuse(result)

#     # een bachelordiploma:
#     result = _add_een_diploma(result, bachelordiplomas)
#     result = _add_dit_diploma(result, bachelordiplomas)
#     result = _add_status_student(result)
#     print("Voeg Cohort toe")
#     result["Cohort"] = result["Inschrijvingsjaar"]
#     result["CohortType"] = result["InPACohortDefinitie"].replace({"Ja": "EersteKeerHO", "Nee": "EersteKeerHsl"})
#     result["TypeOpleiding"] = result.TypeHogerOnderwijsBinnenSoortHogerOnderwijs.replace(
#         {"ba": "bachelor", "ma": "master", "ad": "associate degree"}
#     )

#     return result

def create_cohorten_met_indicatoren(source_dir: Path, eencijfer: pd.DataFrame) -> pd.DataFrame:
    """Create cohorten-table from all parts.

    Returns:
        pd.DataFrame: Cohort table with indicators for the first year.
    """
    eencijfer = _create_eencijfer_df(source_dir)
    print("DEBUG: Starting create_cohorten_met_indicatoren function")

    instroom = create_actief_hoofd_eerstejaar_instelling(source_dir, eencijfer)
    print(f"DEBUG: After create_actief_hoofd_eerstejaar_instelling, instroom has {len(instroom)} rows.")

    inschrijvingen_tweede_jaar = create_inschrijving_jaar2(source_dir, eencijfer)
    print(f"DEBUG: After create_inschrijving_jaar2, inschrijvingen_tweede_jaar has {len(inschrijvingen_tweede_jaar)} rows.")

    propedeusediplomas = create_propedeuse_diplomas(eencijfer)
    print(f"DEBUG: After create_propedeuse_diplomas, propedeusediplomas has {len(propedeusediplomas)} rows.")

    bachelordiplomas = create_bachelordiplomas(eencijfer)
    print(f"DEBUG: After create_bachelordiplomas, bachelordiplomas has {len(bachelordiplomas)} rows.")

    result = merge_cohort_inschrijving_jaar2(instroom, inschrijvingen_tweede_jaar)
    print(f"DEBUG: After merge_cohort_inschrijving_jaar2, result has {len(result)} rows.")

    result["UitvalEerstejaar"] = np.where(
        ((result.ActueleInstelling == result.ActueleInstelling_2ejaar) | (result.HoDiplomaInEersteJaar == 1)),
        0,
        1,
    )
    result["opleiding_gelijk"] = np.where(result.opleiding == result.opleiding_2ejaar, 1, 0)
    result["opleiding_gelijk"] = np.where(
        result.OpleidingActueelEquivalent == result.OpleidingActueelEquivalent_2ejaar,
        1,
        result.opleiding_gelijk,
    )
    result["HerinschrijvingInstelling"] = np.where(
        ((result.opleiding_gelijk == 1) & (result.HoDiplomaInEersteJaar == 0)), 1, 0
    )
    result["HerinschrijvingInstelling"] = np.where((result.ActueleInstelling == result.ActueleInstelling_2ejaar), 1, 0)

    result["SwitchBinnenInstelling"] = np.where(
        ((result.opleiding_gelijk == 0) & (result.UitvalEerstejaar == 0) & (result.HoDiplomaInEersteJaar == 0)),
        1,
        0,
    )

    if not len(instroom) == len(result):
        print("DEBUG: Warning - length of instroom does not match length of result after merging and calculations.")
        raise Exception('Something went wrong with merge.')

    result = add_propedeuse_in_1_jaar(result, eencijfer)
    print(f"DEBUG: After add_propedeuse_in_1_jaar, result has {len(result)} rows.")

    result = _add_herinschrijver_met_propedeuse(result)
    print(f"DEBUG: After _add_herinschrijver_met_propedeuse, result has {len(result)} rows.")

    result = _add_een_diploma(result, bachelordiplomas)
    print(f"DEBUG: After _add_een_diploma, result has {len(result)} rows.")

    result = _add_dit_diploma(result, bachelordiplomas)
    print(f"DEBUG: After _add_dit_diploma, result has {len(result)} rows.")

    result = _add_status_student(result)
    print(f"DEBUG: After _add_status_student, result has {len(result)} rows.")

    print("DEBUG: Adding Cohort information")
    result["Cohort"] = result["Inschrijvingsjaar"]
    result["CohortType"] = result["InPACohortDefinitie"].replace({"Ja": "EersteKeerHO", "Nee": "EersteKeerHsl"})
    result["TypeOpleiding"] = result.TypeHogerOnderwijsBinnenSoortHogerOnderwijs.replace(
        {"ba": "bachelor", "ma": "master", "ad": "associate degree"}
    )

    if result.empty:
        print("DEBUG: The resulting table is empty. Check the intermediate steps for potential issues.")
    else:
        print(f"DEBUG: The final resulting table has {len(result)} rows.")

    return result
