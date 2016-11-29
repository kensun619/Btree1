/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	std::cout << "calling BTreeIndex constructor" << std::endl;
	bufMgr = bufMgrIn;
	
	// create index file name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();
	// std::cout << outIndexName << std::endl;
	/*
	First, if the index file does not exist: 
    create new BlobFile.
    'allocate' new meta page 
    'allocate' new root page
    populate 'metaIndexInfo' with the rootpage num
    scan records and insert into the BTree 
	*/
	 std::cout << outIndexName << " " << File::exists(outIndexName) << std::endl;
	if (File::exists(outIndexName))
	{
		
		// store the file objecthy
		BlobFile blobFile = BlobFile::create(outIndexName);
		PageId metaPageId;
		PageId rootPageId;
		Page metaPage = blobFile.allocatePage(metaPageId);
		headerPageNum = metaPageId;
		rootPageNum = rootPageId;
		Page rootPage = blobFile.allocatePage(rootPageId);
		IndexMetaInfo metaInfo;
		relationName.copy(metaInfo.relationName,relationName.length());
		metaInfo.attrType = attrType;
		metaInfo.rootPageNo = rootPageId;

		memcpy(&metaPage, &metaInfo, sizeof(IndexMetaInfo));
		//scanning

		FileScan fscan(relationName, bufMgr);
		
		try
		{
			std::cout << "index file does not exist, start scanning relation file" << std::endl;
			RecordId scanRid;
			while(1)
			{

				fscan.scanNext(scanRid);
				//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record. 
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				//int key = *((int *)(record + attrByteOffset));
				int key = *((int *)(record));
				std::cout << "Extracted : " << key << std::endl;
			}
		}
		catch(EndOfFileException e)
		{
			std::cout << "Read all records" << std::endl;
		}
		
	}
	//fscan is out of scope
	/*
	read the first page from the file - which is the meta node
    get the root page num from the meta node
    read the root page (bufManager->readPage(file, rootpageNum, out_root_page)
    once you have the root node, you can traverse down the tree 
	*/
	else
	{
	    BlobFile tmpFile = BlobFile::open(outIndexName);

	   
	}



	// while(1)
	// {
	// 	Page *nextPage;
	// 		bufMgr->readPage(file, nextPageId, nextPage);

	// 		LeafNodeInt* leafNode = reinterpret_cast< LeafNodeInt* >(nextPage);
	// }
	

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
