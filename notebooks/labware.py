import json

import pandas as pd
import requests
from pandas import json_normalize

host = "http://www.aquamonitor.no/"
aqua_site = "AquaServices"
archive_site = "AquaServices"
cache_site = "AquaCache"


def reportJsonError(response):
    message = (
        "AquaMonitor failed with status: " + str(response.status_code) + " and message:"
    )
    if response.text is not None:
        try:
            message = message + "\n\n" + json.loads(response.text).get("Message")
        except:
            message = message + "\nNo JSON in response."

    raise Exception(message)


def getJson(token, path):
    response = requests.get(host + path, cookies=dict(aqua_key=token))
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        reportJsonError(response)


def queryGraph(token, document):
    """Interface to GraphQL API."""
    resp = requests.post(
        host + aqua_site + "lab/graphql", json=document, cookies=dict(aqua_key=token)
    )

    if "errors" in resp.json():
        message = resp.json()["errors"]
        raise Exception(message)

    return resp.json()["data"]


def get_labware_projects(token, proj_code):
    """Get all Labware 'projects' for the specified NIVA project.

    Args:
        token:     Obj. Valid authentication token for AM
        proj_code: Str. NIVA project code e.g. '190091;3' for the 1000 Lakes

    Returns:
        Dataframe
    """
    proj_code = str(proj_code)

    resp = queryGraph(
        token,
        {
            "query": "query getProjects($nr: String) {projects(projectNr: $nr){name,status,closed}}",
            "variables": {"nr": proj_code},
        },
    )
    return json_normalize(resp["projects"])


def get_labware_project_samples(token, proj_list):
    """Get all Labware samples for the specified list of Labware projects.

    Args:
        token:     Obj. Valid authentication token for AM
        proj_list: Iterable. List of Labware project names

    Returns:
        Dataframe
    """
    # Containers for data
    df_list = []
    lw_id_list = []
    id_list = []
    name_list = []
    type_list = []

    for proj in proj_list:
        # Get sample data
        resp = queryGraph(
            token,
            {
                "query": "query getSamples($name: String) {samples(projectName: $name){sampleNumber,textID,"
                "projectStationId,status,sampledDate,sampleDepthUpper,sampleDepthLower}}",
                "variables": {"name": proj},
            },
        )

        resp = json_normalize(resp["samples"])
        df_list.append(resp)

        if len(resp) > 0:
            for stn_id in resp["projectStationId"].unique():
                try:
                    stn_data = getJson(token, aqua_site + f"api/stations/{stn_id}")

                    lw_id_list.append(stn_id)
                    id_list.append(stn_data["Id"])
                    name_list.append(stn_data["Name"])
                    type_list.append(stn_data["Type"]["_Text"])

                except Exception as e:
                    print(f"Error identifying projectStationId {stn_id}:")
                    print(e)
                    print("###############################################")
                    pass

    samp_df = pd.concat(df_list, axis="rows", sort=True)

    stn_df = pd.DataFrame(
        {
            "projectStationId": lw_id_list,
            "station_id": id_list,
            "station_name": name_list,
            "station_type": type_list,
        }
    )
    stn_df.drop_duplicates(inplace=True)

    samp_df = pd.merge(
        samp_df,
        stn_df,
        how="left",
        on="projectStationId",
    )

    samp_df = samp_df[
        [
            "sampleNumber",
            "textID",
            "projectStationId",
            "status",
            "sampledDate",
            "sampleDepthUpper",
            "sampleDepthLower",
            "station_id",
            "station_name",
            "station_type",
        ]
    ]

    return samp_df


def get_labware_sample_results(token, samp_list):
    """Get all Labware results for the specified list of samples.

    Args:
        token:     Obj. Valid authentication token for AM
        samp_list: Iterable. List of Labware sampleNumbers

    Returns:
        Dataframe
    """
    df_list = []

    for samp in samp_list:
        assert isinstance(samp, int), "sampleNumbers must be integers."

        resp = queryGraph(
            token,
            {
                "query": "query getResults($nr: Int) {results(sampleNumber: $nr){name,units,analysis,test{anaFraction},accreditedId,entryQualifier,"
                "numericEntry,mu,loq,status}}",
                "variables": {"nr": samp},
            },
        )

        res_df = json_normalize(resp["results"])
        res_df["sample_id"] = samp
        df_list.append(res_df)

    res_df = pd.concat(df_list, axis="rows", sort=True)

    return res_df
