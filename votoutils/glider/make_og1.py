import xarray as xr
from votoutils.glider.convert_to_og1 import convert_to_og1, standardise_og10
import subprocess
from pathlib import Path
import requests


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


def multi():
    glidermissions = (
        # (77, 21),
        # (55, 78),
        # (55, 82),
        # (77, 11),
        # (45, 79),
        # (69, 15),
        # (67, 37),
        (76, 17),
        # (63, 63),
    )
    for glider, mission in glidermissions:
        mission_str = f"SEA0{glider} M{mission}"
        print(f"proc {glider} {mission}")
        data_source = f"https://erddap.observations.voiceoftheocean.org/erddap/files/delayed_SEA0{glider}_M{mission}/mission_timeseries.nc"
        data_dir = Path("/home/callum/Downloads/datasets/og1")
        outfile = data_dir / f"tmp_{mission_str}.nc"
        if outfile.exists():
            print("Done ", mission_str)
            return
        data_file = data_dir / (mission_str + ".nc")
        if not data_file.exists():
            req = requests.get(data_source)
            with open(data_file, "wb") as wfile:
                wfile.write(req.content)
        ds = xr.open_dataset(data_file)
        ds_standard = standardise_og10(ds)
        ds_og1 = convert_to_og1(ds_standard)
        outfile = data_dir / f"{ds_og1.attrs['id']}.nc"
        ds_og1.to_netcdf(outfile)


def single():
    # ds = xr.open_dataset("/data/data_l0_pyglider/complete_mission/SEA76/M19/timeseries/mission_timeseries.nc")
    # ds = xr.open_dataset("/data/data_l0_pyglider/complete_mission/SEA55/M31/timeseries/mission_timeseries.nc")
    ds = xr.open_dataset(
        "/data/data_l0_pyglider/complete_mission/SEA45/M79/timeseries/mission_timeseries.nc"
    )
    ds = ds.sel(
        time=slice(ds.time.values[459416], ds.time.values[470647])
    )  #     ds = ds.sel(time=slice(ds.time.values[439416], ds.time.values[470647]))
    # ds = xr.open_dataset("/data/data_l0_pyglider/nrt/SEA55/M31/timeseries/mission_timeseries.nc")
    # ds = xr.open_dataset(
    #    "/data/data_l0_pyglider/complete_mission/SEA44/M33/timeseries/mission_timeseries.nc",
    # )
    ds_standard = standardise_og10(ds)
    ds_og1 = convert_to_og1(ds_standard)
    outfile = f"/home/callum/Documents/community/OG-format-user-manual/og_format_examples_files/{ds_og1.attrs['id']}.nc"
    print(ds_og1.attrs["id"])
    cdl = f"/home/callum/Documents/community/OG-format-user-manual/og_format_examples_files/{ds_og1.attrs['id']}.cdl"
    ds_og1.to_netcdf(outfile)
    my_cmd = ["ncdump", outfile]
    with open(cdl, "w") as outfile:
        subprocess.run(my_cmd, stdout=outfile)


if __name__ == "__main__":
    multi()
    # lots()
