def extract_relevant_text(data):
    ps = data.get("protocolSection", {})
    lines = []

    def get(*keys, default=""):
        d = ps
        for key in keys:
            d = d.get(key, {})
        return d.get("text", default) if isinstance(d, dict) else d if isinstance(d, str) else default

    lines.append(f"Official Title: {get('identificationModule', 'officialTitle')}")
    lines.append(f"Detailed Description: {get('descriptionModule', 'detailedDescription')}")

    status = ps.get("statusModule", {})
    for label, date in [
        ("Start Date", status.get("startDateStruct", {}).get("date")),
        ("Primary Completion Date", status.get("primaryCompletionDateStruct", {}).get("date")),
        ("Study Completion Date", status.get("completionDateStruct", {}).get("date"))
    ]:
        if date:
            lines.append(f"{label}: {date}")

    party = ps.get("sponsorCollaboratorsModule", {}).get("responsibleParty", {})
    for key, label in [
        ("investigatorFullName", "Investigator"),
        ("investigatorTitle", "Title"),
        ("investigatorAffiliation", "Affiliation")
    ]:
        if party.get(key):
            lines.append(f"{label}: {party[key]}")

    conditions = ps.get("conditionsModule", {}).get("conditions", [])
    if conditions:
        lines.append(f"Conditions: {', '.join(conditions)}")

    study_type = ps.get("designModule", {}).get("studyType")
    if study_type:
        lines.append(f"Study Type: {study_type}")

    phases = ps.get("designModule", {}).get("phases", [])
    if phases:
        lines.append(f"Phase(s): {', '.join(phases)}")

    interventions = ps.get("armsInterventionsModule", {}).get("interventions", [])
    if interventions:
        names = [i.get("name") for i in interventions if "name" in i]
        lines.append(f"Interventions: {', '.join(names)}")

    outcomes = ps.get("outcomesModule", {}).get("primaryOutcomes", [])
    if outcomes:
        lines.append("Primary Outcomes:")
        for outcome in outcomes:
            line = f"- {outcome.get('measure', '').strip()}"
            if outcome.get("timeFrame"): line += f" ({outcome['timeFrame'].strip()})"
            if outcome.get("description"): line += f": {outcome['description'].strip()}"
            lines.append(line)

    return "\n".join([line for line in lines if line.strip() and "None" not in line])
