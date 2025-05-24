import os
import tempfile
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

import urllib3
from doc2json.grobid_client import GrobidClient
from dotenv import load_dotenv
from urllib3 import PoolManager
from urllib3.util.retry import Retry

from minio import Minio  # type: ignore

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv(override=True)

BUCKET_NAME = 'pubmed'

input_files = [
    "00001-2022.PMC9108969.pdf",
    "00002-2022.PMC9108967.pdf",
    "00004-2022.PMC8994962.pdf",
    "00004-2023.PMC10259823.pdf",
    "00006-2022.PMC8899496.pdf",
    "00007-2023.PMC10086694.pdf",
    "00008-2022.PMC9379355.pdf",
    "00009-2022.PMC9149393.pdf",
    "0001.f1.pdf",
    "0001.f2.pdf",
    "00010-2022.PMC9059131.pdf",
    "00010-2023.PMC10277870.pdf",
    "00011-2023.PMC10204863.pdf",
    "00012-2022.PMC9501655.pdf",
    "00013-2023.PMC10204849.pdf",
    "00014-2022.PMC8845010.pdf",
    "00016-2022.PMC9124870.pdf",
    "00017-2022.PMC8995535.pdf",
    "00017-2023.PMC10423983.pdf",
    "00020-2023.PMC10204814.pdf",
    "00021-2023.pdf",
    "00024-2022.PMC9574553.pdf",
    "00024-2023.PMC10316040.pdf",
    "00025-2022.PMC9501840.pdf",
    "00026-2023.PMC10204812.pdf",
    "00027-2023.PMC10440677.pdf",
    "00028-2022.PMC9234436.pdf",
    "00030-2022.PMC9339768.pdf",
    "00031-2022.PMC9062297.pdf",
    "00032-2022.PMC8958218.pdf",
    "00033-2022.PMC9511143.pdf",
    "00033-2023.PMC10227635.pdf",
    "00034-2022.PMC8883039.pdf",
    "00034-2023.PMC10204859.pdf",
    "00035-2022.PMC9108963.pdf",
    "00035-2023.PMC10518870.pdf",
    "00036-2023.PMC10291303.pdf",
    "00038-2022.PMC9149385.pdf",
    "00039-2023.PMC10204822.pdf",
    "0004-282X-anp-80-08-0767.PMC9703882.pdf",
    "0004-282X-anp-80-08-0770.PMC9703896.pdf",
    "0004-282X-anp-80-08-0779.PMC9703884.pdf",
    "0004-282X-anp-80-08-0786.PMC9703892.pdf",
    "0004-282X-anp-80-08-0794.PMC9703890.pdf",
    "0004-282X-anp-80-08-0802.PMC9703888.pdf",
    "0004-282X-anp-80-08-0806.PMC9703885.pdf",
    "0004-282X-anp-80-08-0812.PMC9703889.pdf",
    "0004-282X-anp-80-08-0822.PMC9703895.pdf",
    "0004-282X-anp-80-08-0831.PMC9703883.pdf",
    "0004-282X-anp-80-08-0837.PMC9703894.pdf",
    "0004-282X-anp-80-08-0845.PMC9703891.pdf",
    "0004-282X-anp-80-08-0862.PMC9703887.pdf",
    "0004-282X-anp-80-08-0867.PMC9703881.pdf",
    "0004-282X-anp-80-08-0869.PMC9703893.pdf",
    "0004-282X-anp-80-08-0871.PMC9703886.pdf",
    "00044-2022.PMC9339767.pdf",
    "00046-2022.PMC9080287.pdf",
    "00046-2023.PMC10277871.pdf",
    "00048-2022.PMC9271757.pdf",
    "00048-2023.pdf",
    "00051-2023.PMC10152258.pdf",
    "00052-2023.PMC10107065.pdf",
    "00053-2022.PMC9149391.pdf",
    "00053-2023.PMC10204811.pdf",
    "00054-2022.PMC9209851.pdf",
    "00055-2023.PMC10291312.pdf",
    "00056-2022.PMC9035766.pdf",
    "00056-2023.PMC10645323.pdf",
    "00057-2022.SUPPLEMENT.pdf",
    "00057-2022.pdf",
    "00057-2023.PMC10493709.pdf",
    "00058-2022.SUPPLEMENT.pdf",
    "00058-2022.pdf",
    "00060-2022.PMC9835995.pdf",
    "00061-2022.PMC9209849.pdf",
    "00063-2022.PMC9234440.pdf",
    "00063-2023.PMC9969230.pdf",
    "00064-2023.PMC10316043.pdf",
    "00065-2022.PMC8994963.pdf",
    "00066-2022.PMC8899494.pdf",
    "00067-2022.PMC9661281.pdf",
    "00068-2022.PMC9209848.pdf",
    "00069-2022.PMC8982749.pdf",
    "00070-2023.PMC10291313.pdf",
    "00072-2022.PMC8943283.pdf",
    "00072-2023.PMC10227628.pdf",
    "00074-2022.SUPPLEMENT.pdf",
    "00074-2022.pdf",
    "00074-2023.PMC10493712.pdf",
    "00075-2022.PMC9309344.pdf",
    "00078-2023.PMC10291305.pdf",
    "00079-2023.PMC10291310.pdf",
    "00080-2022.PMC9574560.pdf",
    "00080-2023.PMC10291299.pdf",
    "00082-2022.PMC8990384.pdf",
    "00082-2023.PMC10493707.pdf",
    "00085-2022.PMC9703146.pdf",
    "00087-2023.PMC10152263.pdf",
    "00090-2023.PMC10227632.pdf",
    "00092-2022.PMC9168080.pdf",
    "00093-2022.PMC9271754.pdf",
    "00094-2023.PMC10204820.pdf",
    "00095-2022.PMC9149384.pdf",
    "00098-2023.PMC10440648.pdf",
    "00100-2023.PMC10204816.pdf",
    "00102-2023.PMC10388177.pdf",
    "00104-2023.PMC10204853.pdf",
    "00105-2022.PMC9289374.pdf",
    "00109-2022.PMC9251366.pdf",
    "00110-2022.SUPPLEMENT.pdf",
    "00110-2022.pdf",
    "00111-2022.PMC9131135.pdf",
    "00113-2022.PMC9235056.pdf",
    "00114-2022.PMC9530886.pdf",
    "00115-2022.PMC9209850.pdf",
    "00116-2022.PMC9124868.pdf",
    "00117-2022.PMC9209847.pdf",
    "00117-2023.PMC10276923.pdf",
    "00119-2022.PMC9149388.pdf",
    "00120-2022.PMC9465005.pdf",
    "00121-2022.pdf",
    "00122-2022.PMC9589319.pdf",
    "00122-2023.PMC10107053.pdf",
    "00123-2023.PMC10291304.pdf",
    "00124-2023.PMC10204731.pdf",
    "00126-2022.PMC9234427.pdf",
    "00127-2022.PMC9234439.pdf",
    "00127-2023.PMC10463038.pdf",
    "00129-2022.PMC9131124.pdf",
    "00131-2022.SUPPLEMENT.pdf",
    "00131-2022.pdf",
    "00131-2023.PMC10518878.pdf",
    "00132-2022.PMC9619251.pdf",
    "00135-2023.PMC10423986.pdf",
    "00138-2022.PMC9511155.pdf",
    "00138-2023.PMC10641575.pdf",
    "00139-2022.PMC9379353.pdf",
    "00140-2022.FIGURE1.pdf",
    "00140-2022.FIGURE2.pdf",
    "00140-2022.SUPPLEMENT.pdf",
    "00140-2022.pdf",
    "00141-2022.PMC9379352.pdf",
    "00141-2023.PMC10493713.pdf",
    "00142-2022.PMC9309342.pdf",
    "00143-2023.PMC10463028.pdf",
    "00144-2022.pdf",
    "00144-2022.supplement.pdf",
    "00145-2023.PMC10316044.pdf",
    "00146-2022.PMC9793243.pdf",
    "00148-2023.PMC10316033.pdf",
    "00150-2022.PMC9234426.pdf",
    "00152-2022.PMC9501643.pdf",
    "00154-2021.PMC8841990.pdf",
    "00154-2022.PMC9209852.pdf",
    "00156-2023.PMC10291309.pdf",
    "00157-2022.PMC9703150.pdf",
    "00158-2023.PMC10388176.pdf",
    "00159-2023.PMC10518872.pdf",
    "00161-2023.pdf",
    "00163-2022.PMC9574559.pdf",
    "00163-2023.PMC10518857.pdf",
    "00164-2022.PMC9835973.pdf",
    "00164-2023.PMC10463025.pdf",
    "00165-2022.PMC9589321.pdf",
    "00167-2023.PMC10423982.pdf",
    "00168-2022.PMC9530887.pdf",
    "00168-2023.PMC10105511.pdf",
    "00170-2022.SUPPLEMENT.pdf",
    "00170-2022.pdf",
    "00172-2022.PMC9619250.pdf",
    "00174-2022.PMC9661269.pdf",
    "00176-2023.PMC10505954.pdf",
    "00177-2023.PMC10227636.pdf",
    "00179-2022.PMC9149383.pdf",
    "00179-2023.PMC10463030.pdf",
    "00180-2023.PMC10613990.pdf",
    "00181-2023.PMC10291725.pdf",
    "00182-2023.PMC10277874.pdf",
    "00183-2022.pdf",
    "00185-2022.PMC9309343.pdf",
    "00186-2022.PMC9589324.pdf",
    "00186-2023.PMC10316034.pdf",
    "00189-2022.PMC9511157.pdf",
    "00190-2022.PMC9720546.pdf",
    "00190-2023.PMC10440652.pdf",
    "00191-2022.PMC9589331.pdf",
    "00193-2022.PMC9703145.pdf",
    "00193-2023.PMC10316041.pdf",
    "00194-2023.PMC10423988.pdf",
    "00195-2023.PMC10658642.pdf",
    "00196-2022.PMC9548241.pdf",
    "00199-2022.PMC9271262.pdf",
    "00200-2022.PMC9511138.pdf",
    "00202-2022.PMC9234424.pdf",
    "00203-2022.PMC9234430.pdf",
    "00203-2023.PMC10440649.pdf",
    "00204-2022.PMC9661249.pdf",
    "00205-2022.PMC9792102.pdf",
    "00206-2022.pdf",
    "00206-2023.PMC10505950.pdf",
]

# Create a custom PoolManager with desired settings
http_client = PoolManager(
    timeout=300,  # 5 minutes timeout
    maxsize=200,  # Increased pool size
    retries=Retry(total=10, backoff_factor=0.1, status_forcelist=[100, 102, 103, 104]))

minio_client = Minio(
    os.environ['MINIO_API_ENDPOINT'],
    access_key=os.getenv('MINIO_ACCESS_KEY'),
    secret_key=os.getenv('MINIO_SECRET_KEY'),
    secure=True,
    http_client=http_client,
)

BASE_TEMP_DIR = 'temp'

NUM_PARALLEL = 120

# Configure Grobid
grobid_config = {
    # "grobid_server": os.getenv('GROBID_SERVER'),
    # "grobid_server": 'https://grobid-gpub004.kastan.ai/',
    "grobid_server": 'https://grobid-cn037.kastan.ai/',
    "batch_size": 2000,
    "sleep_time": 3,
    "generateIDs": False,
    "consolidate_header": False,
    "consolidate_citations": False,
    # "include_raw_citations": True,
    "include_raw_citations": False,
    "include_raw_affiliations": False,
    "timeout": 600,
    "n": NUM_PARALLEL,
    "max_workers": NUM_PARALLEL,
    # "concurrency": 8,
}

# Process PDF with Grobid
grobidClient = GrobidClient(grobid_config)


def process_pdf(file_name):
  # Download file from Minio
  start_time_minio = time.monotonic()
  start_time_grobid = time.monotonic()
  response = minio_client.get_object(BUCKET_NAME, file_name)
  file_content = response.read()
  response.close()
  response.release_conn()
  print(f"⏰ Minio download: {(time.monotonic() - start_time_minio):.2f} seconds")

  # Create a temporary file
  with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_pdf:
    temp_pdf.write(file_content)
    temp_pdf_path = temp_pdf.name

  # Save to a persistent file
  persistent_path = os.path.join('grobid_speedtest_pdfs', file_name)
  os.makedirs(os.path.dirname(persistent_path), exist_ok=True)
  with open(persistent_path, 'wb') as persistent_pdf:
    persistent_pdf.write(file_content)

  # Create a temporary file for TEI output
  with tempfile.NamedTemporaryFile(delete=False, suffix='.tei.xml') as temp_tei:
    temp_tei_path = temp_tei.name

  grobidClient.process_pdf(temp_pdf_path, temp_tei_path, "processFulltextDocument")

  # Clean up temporary files
  os.unlink(temp_pdf_path)
  os.unlink(temp_tei_path)
  print(f"📜 Grobid runtime: {(time.monotonic() - start_time_grobid):.2f} seconds")

  return f"Processed {file_name}"


def main():
  start_time = time.monotonic()

  with ProcessPoolExecutor(max_workers=NUM_PARALLEL) as executor:
    futures = [executor.submit(process_pdf, file_name) for file_name in input_files]

    for future in as_completed(futures):
      try:
        result = future.result()
        print(result)
      except Exception as e:
        print(f"An error occurred: {str(e)}")
        traceback.print_exc()

  print(f"Total runtime: {(time.monotonic() - start_time) / 60:.2f} minutes")


if __name__ == "__main__":
  main()
