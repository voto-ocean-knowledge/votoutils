import xarray as xr
from votoutils.glider.convert_to_og1 import convert_to_og1, standardise_og10
import subprocess


def lots():
    from erddapy import ERDDAP

    e = ERDDAP(
        server="https://erddap.observations.voiceoftheocean.org/erddap",
        protocol="tabledap",
    )
    e.dataset_id = "allDatasets"
    df = e.to_pandas()
    df_nrt = df[df["datasetID"].str[:3] == "nrt"]
    for ds_id in df_nrt["datasetID"].values:
        # if 'SEA070' in ds_id:
        #    print("skipping sea70")
        #    continue
        print(ds_id)
        e.dataset_id = ds_id
        ds = e.to_xarray().drop_dims("timeseries")
        ds_standard = standardise_og10(ds)
        ds_og1 = convert_to_og1(ds_standard)
        print(ds_og1.attrs["title"])


def single():
    # ds = xr.open_dataset("/data/data_l0_pyglider/complete_mission/SEA76/M19/timeseries/mission_timeseries.nc")
    ds = xr.open_dataset(
        "/data/data_l0_pyglider/complete_mission/SEA44/M33/timeseries/mission_timeseries.nc",
    )
    ds_standard = standardise_og10(ds)
    ds_og1 = convert_to_og1(ds_standard)
    outfile = f"/home/callum/Documents/community/OG-format-user-manual/og_format_examples_files/{ds_og1.attrs['id']}.nc"
    cdl = f"/home/callum/Documents/community/OG-format-user-manual/og_format_examples_files/{ds_og1.attrs['id']}.cdl"
    ds_og1.to_netcdf(outfile)
    my_cmd = ["ncdump", outfile]
    with open(cdl, "w") as outfile:
        subprocess.run(my_cmd, stdout=outfile)


if __name__ == "__main__":
    lots()
