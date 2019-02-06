import xmlschema
from pprint import pprint
from lxml import etree
import os
import pdfx
from threading import Thread
from queue import Queue
from pymongo import MongoClient
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-r", "--recover", action='store_true', dest="recovery", default=True,
                  help="resume downloading data from the last inserted document of mongo")
(options, args) = parser.parse_args()

recovery_id=0

MAX_FETCH_THREADS=50
WORKER_THREADS=5


q = Queue(maxsize=0)
thread_pool = []

class DataInsertThread(Thread):

    database = None
    thread_id = 1

    def __init__(self, db, queue, _element_tree, recovery_id_offset=0):
        self.data = False
        self._element_tree = _element_tree
        self.database = db
        self.queue = queue
        self.arxiv_id = None

        if _element_tree is not None:
            self.thread_id = DataInsertThread.thread_id + recovery_id_offset
            DataInsertThread.thread_id += 1

        Thread.__init__(self)

    def _getData(self):
        # ensure we only do work when we need to
        if self.data:
            return

        element_tree = etree.XML(etree.tostring(self._element_tree))
        arxiv_id = element_tree.xpath("//*[local-name()='id']/text()")[0]
        self.arxiv_id = arxiv_id
        arxiv_abstract = element_tree.xpath("//*[local-name()='abstract']/text()")[0]

        arxiv_pdf_link = self.get_pdf_link_from_arxiv_id(arxiv_id)
        arxiv_pdf_contents = pdfx.PDFx(arxiv_pdf_link)
        
        arxiv_pdf_contents_text = arxiv_pdf_contents.get_text()
        arxiv_pdf_contents_references = arxiv_pdf_contents.get_references_as_dict()
        arxiv_pdf_contents_metadata = arxiv_pdf_contents.get_metadata()

        _data = {
            "id" : arxiv_id,
            "abstract" : arxiv_abstract,
            "text" : arxiv_pdf_contents_text,
            "references" : arxiv_pdf_contents_references,
            "metadata" : arxiv_pdf_contents_metadata,
            "recovery_id" : self.thread_id
        }

        self.data = True
        print('thread about to queue data')
        self.queue.put(_data)
        print ("THREAD CLEAN-UP: "+ self.arxiv_id)

    def _getQueueItemsAndInsert(self):
        print("worker thread initialized")
        while True:
            queue_data = self.queue.get()

            # if the thread doesn't complete, we don't store the data
            if queue_data is None:
                print("data was none...")
                self.queue.task_done()
                return
            
            print("storing data from queue")
            self.database.papers.insert(queue_data, check_keys=False)
            self.queue.task_done()

    def run(self):
        # if the thread doesn't have an element tree, it's a worker thread
        if self._element_tree is None:
            self._getQueueItemsAndInsert()

        # otherwise its a getter thread
        else:
            self._getData()

    def get_pdf_link_from_arxiv_id(self, arxiv_id):
        base = "https://arxiv.org/pdf/{0}.pdf"
        return base.format(arxiv_id)


def get_last_inserted_arxiv_id():
    global recovery_id
    last_inserted_paper = databaseObject.papers.find().sort([('recovery_id', -1)]).limit(1)
    if last_inserted_paper.count() == 0:
        return None
    recovery_id = last_inserted_paper[0]["recovery_id"]
    return last_inserted_paper[0]["id"]


def iterate_xml(xmlfile):
    global options
    last_inserted_arxiv_id = None

    if options.recovery:
        skip_iter = True
        last_inserted_arxiv_id = get_last_inserted_arxiv_id()
        print ("----------------RECOVERY IN PROGRESS----------------")

    else:
        skip_iter = False

    doc = etree.iterparse(xmlfile, events=('start', 'end'))
    _, root = next(doc)
    start_tag = None
    for event, element in doc:
        if event == 'start' and start_tag is None:
            start_tag = element.tag
        if event == 'end' and element.tag == start_tag:
            if skip_iter:
                # need to compare arxiv id of current _element tree of arxiv id to last mongo
                if last_inserted_arxiv_id is None:
                    print('could not find any arxiv id in DB')
                    skip_iter = False
                    yield element

                else:
                    element_tree = etree.XML(etree.tostring(element))
                    arxiv_id = element_tree.xpath("//*[local-name()='id']/text()")[0]
                    if arxiv_id.strip().lower() == last_inserted_arxiv_id.strip().lower():
                        print ('arxiv id matched: ' + arxiv_id)
                        print ("----------------RECOVERY SUCCESSFUL----------------")
                        skip_iter = False
                    else:
                        print ('skipping iteration. arxiv id: ' + arxiv_id)

            else:
                yield element

            start_tag = None
            root.clear()


def check_thread_completion():
    global thread_pool
    for t in thread_pool:
        if not t.isAlive():
            print('clean-up dead thread')
    thread_pool = [t for t in thread_pool if t.isAlive()]

mongoClient = MongoClient("mongodb://localhost:27017/",
    maxPoolSize=MAX_FETCH_THREADS, # connection pool size
    waitQueueTimeoutMS=1000, # how long a thread can wait for a connection
    waitQueueMultiple=MAX_FETCH_THREADS # when the pool is fully used N threads can wait
    )

# Get the database object
databaseObject = mongoClient.ThoughtDB_ARXIV

arxiv_metadata_local_source = './arxiv-bulk-metadata/arxiv_biblio_arXiv.2018-01-19.xml'

# these threads will get from the queue and put data into mongo
for i in range(WORKER_THREADS):
    worker = DataInsertThread(databaseObject, q, None)
    worker.setDaemon(True)
    worker.start()

# these threads will fetch data from arxiv
# parse the PDF and add to the queue
for _element_tree in iterate_xml(arxiv_metadata_local_source):
    # block when our thread pool is full until a thread is finished
    while (len(thread_pool) >= MAX_FETCH_THREADS):
        check_thread_completion()

    print("starting getter thread")
    getter = DataInsertThread(databaseObject, q, _element_tree, recovery_id_offset=recovery_id)
    getter.setDaemon(True)
    getter.start()
    thread_pool.append(getter)

while True:
    pass


