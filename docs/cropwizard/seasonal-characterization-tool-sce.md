# Seasonal Characterization Tool (SCE)

The **Seasonal Characterization tool** brings crop simulation into CropWizard. Give it a list of field trials — where each trial was planted, when, and with what cultivar maturity — and it runs the [SCE (Seasonal Characterization Engine)](https://github.com/CatherineGilbert/SCE) with the APSIM crop model behind the scenes to describe what the growing-season environment was like at each site, **broken down by crop development stage**.

For every trial you get, per development phase (emergence, flowering, grain fill, and so on):

* **Rainfall** and accumulated **thermal time**
* **Temperature** and **solar radiation**
* **Plant-available water** in the soil
* **Water, temperature, and nutrient stress** experienced by the crop

Alongside the per-stage breakdown, the tool returns season totals, two charts (a heatmap of conditions across trials and stages, and a season accumulation timeline), and a downloadable zip of the underlying CSV outputs, so you can take the numbers into your own analysis.

Weather comes from public datasets (NASA POWER by default) and soils from SSURGO (default) or ISRIC, so all you need to supply is the trial list itself.

## How to use it

{% stepper %}
{% step %}
### Host your trial CSV

Put your trial list in a small CSV file (format below) and host it at a **public `https` link** (up to 5 MB). Any of these work:

* A file in a **public GitHub repository or gist** — use the _Raw_ link.
* A **Google Sheet** shared with "anyone with the link" — use its CSV export link (the sheet's URL with `/export?format=csv` in place of `/edit`).
* A **Dropbox** or similar cloud-storage share link that serves the raw file (redirects are followed).
* Any web server.

_Attaching the file to the chat is not enough yet: the file must be reachable by URL._
{% endstep %}

{% step %}
### Ask about the season

Paste the link into the chat and ask about the season. Mention the crop if your trials are **maize** — soybean is assumed otherwise.

> Characterize the growing season for the soybean trials in https://example.org/my\_trials.csv
>
> Using the trials in this CSV \<link>, how much rain fell during grain fill at each site?
>
> Was my Champaign trial under water stress during flowering? Trials: \<link>
>
> My maize trials are listed at \<link> — what were conditions like at each site, stage by stage?

CropWizard recognizes when the tool is relevant, invokes it automatically, and folds the results into its answer — you will see it under _Routing the request to relevant tools_ and its results under _Tool output_, followed by a final response that interprets the numbers.

Simulations take real work: a typical run finishes in about **30 seconds to a couple of minutes**.
{% endstep %}
{% endstepper %}

## Input CSV format

The file needs exactly these five columns, spelled and capitalized as shown:

```csv
Site,Latitude,Longitude,Genetics,Planting
champaign_il,40.1164,-88.2434,3.5,2021-05-15
urbana_il,40.1106,-88.2073,3.2,2021-05-20
```

| Column                   | What goes in it                                                     |
| ------------------------ | ------------------------------------------------------------------- |
| `Site`                   | A name for the trial (any label you like)                           |
| `Latitude` / `Longitude` | Decimal degrees for the trial location                              |
| `Genetics`               | Cultivar maturity — see below, format differs for soybean and maize |
| `Planting`               | Planting date as `YYYY-MM-DD`                                       |

### Genetics for soybean

A number from −2 to 7.99. The integer part is the maturity group (−2 = 000, 0 = 0, 3 = III) and the decimal part selects early (.0–.33), mid (.34–.66), or late (.67–.99) within the group. So `3.5` means mid maturity group III. Values of 8 and above are not supported.

### Genetics for maize

Relative maturity days, with an optional leading letter: `A_100`, `A100`, or plain `100` all work. Put the letter first if you use one: a trailing letter (`100a`) is not read and the cultivar is treated as late-maturing, with a warning.

### Planting

Use `YYYY-MM-DD`. Slash dates like `5/6/2021` are rejected because they are ambiguous (May 6 or June 5?). A bare year (`2021`) or year and month (`2021-05`) is accepted, but the simulation then covers the whole calendar year and the model picks its own sowing date, so a full date gives much more meaningful results.

The tool checks your file before running and, if something is off, tells you exactly which row and what to fix.

## Good to know

* **Trial count:** designed for **1 to 5 trials** per question, at up to 3 distinct locations. A single site works fine. It characterizes each trial's season; it does not compare or cluster trials against each other.
* **Crops:** soybean and maize.
* **Coverage:** the defaults are NASA POWER weather (global) and SSURGO soil (United States only). For sites outside the US, ask for **ISRIC** soil in your prompt — it is global. Other selectable sources are DAYMET weather (US only) and CHIRPS weather. If site-specific soil data cannot be retrieved for a location, the simulation still runs on the crop model's default soil profile and the response says so.
* **Partial results:** if one trial fails or times out, you still get results for the rest, with an explanation of what went wrong.
* **Downloads:** links to the charts and the CSV outputs are valid for 7 days.

For detailed information on how tools work on Illinois Chat, check out the[ tool-use-in-conversation ](../features/tool-use-in-conversation.md)page.

## Credits

SCE is developed by [Catherine Gilbert](https://github.com/CatherineGilbert/SCE) at the University of Illinois. The CropWizard integration wraps the engine unmodified and adds the service, validation, aggregation, and visualizations.
