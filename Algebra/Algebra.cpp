#include "Algebra.h"
#include <iostream>
#include <cstring>
#include <stdlib.h>

/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into. (ignore for now)
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/

// will return if a string can be parsed as a floating point number
bool isNumber(char *str){

    int len;
    float ignore;

    /*
        sscanf returns the number of elements read, so if there is no float matching
        the first %f, ret will be 0, else it'll be 1

        %n gets the number of characters read. this scanf sequence will read the
        first float ignoring all the whitespace before and after. and the number of
        characters read that far will be stored in len. if len == strlen(str), then
        the string only contains a float with/without whitespace. else, there's other
        characters.
    */

   int ret = sscanf(str,"%f %n",&ignore, &len);

   return ret == 1 && len == strlen(str);

}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]){
    
    int srctargetRelId = OpenRelTable::getRelId(srcRel);

    
    if(srctargetRelId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry;

    int ret = AttrCacheTable::getAttrCatEntry(srctargetRelId,attr,&attrCatEntry);

    // std::cout<<"ret: "<<ret<<std::endl;
    if(ret != SUCCESS){
        return ret;
    }

    /*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/

    int type = attrCatEntry.attrType;

    Attribute attrVal;

    if(type == NUMBER){   // the isNumber() function is implemented below
        if(isNumber(strVal)){
            attrVal.nVal = atof(strVal);
        }else{
            return E_ATTRTYPEMISMATCH;
        }
    }else if(type == STRING){
        strcpy(attrVal.sVal,strVal);
    }

    /*** Selecting records from the source relation ***/

    // Before calling the search function, reset the search to start from the first hit
    // using RelCacheTable::resetSearchIndex()

    RelCatEntry relCatEntry;

    // get relCatEntry using RelCacheTable::getRelCatEntry()

    RelCacheTable::getRelCatEntry(srctargetRelId,&relCatEntry);

     /*** Creating and opening the target relation ***/
    // Prepare arguments for createRel() in the following way:
    // get RelcatEntry of srcRel using RelCacheTable::getRelCatEntry()
    int src_nAttrs = relCatEntry.numAttrs/* the no. of attributes present in src relation */ ;

    char attr_names[src_nAttrs][ATTR_SIZE];

    int attr_types[src_nAttrs];

    /*iterate through 0 to src_nAttrs-1 :
        get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
        fill the attr_names, attr_types arrays that we declared with the entries
        of corresponding attributes
    */

    for(int i=0;i<src_nAttrs;i++){

        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srctargetRelId,i,&attrCatEntry);
        strcpy(attr_names[i],attrCatEntry.attrName);
        attr_types[i] = attrCatEntry.attrType;

    }

    /* Create the relation for target relation by calling Schema::createRel()
       by providing appropriate arguments */
    // if the createRel returns an error code, then return that value.

    ret = Schema::createRel(targetRel,src_nAttrs,attr_names,attr_types);

    if(ret != SUCCESS)return ret;


    /* Open the newly created target relation by calling OpenRelTable::openRel()
       method and store the target targetRelId */
    /* If opening fails, delete the target relation by calling Schema::deleteRel()
       and return the error value returned from openRel() */

    int targettargetRelId = OpenRelTable::openRel(targetRel);

    if(targettargetRelId < 0 || targettargetRelId > MAX_OPEN ){
        Schema::deleteRel(targetRel);
        return targettargetRelId;
    }

    /*** Selecting and inserting records into the target relation ***/
    /* Before calling the search function, reset the search to start from the
       first using RelCacheTable::resetSearchIndex() */

    

    Attribute record[src_nAttrs];

    /*
        The BlockAccess::search() function can either do a linearSearch or
        a B+ tree search. Hence, reset the search index of the relation in the
        relation cache using RelCacheTable::resetSearchIndex().
        Also, reset the search index in the attribute cache for the select
        condition attribute with name given by the argument `attr`. Use
        AttrCacheTable::resetSearchIndex().
        Both these calls are necessary to ensure that search begins from the
        first record.
    */

    RelCacheTable::resetSearchIndex(srctargetRelId);
    AttrCacheTable::resetSearchIndex(srctargetRelId,attr);

    // read every record that satisfies the condition by repeatedly calling
    // BlockAccess::search() until there are no more records to be read

    int count = 0;

    // BlockBuffer::compareCount=0;

    while(true){

        Attribute record[src_nAttrs];

        int ret = BlockAccess::search(srctargetRelId,record,attr,attrVal,op);

        if(ret != SUCCESS)break;

        ret = BlockAccess::insert(targettargetRelId, record);

        count++;
        std::cout<<count<<" inserted!!"<<std::endl;


        // if (insert fails) {
        //     close the targetrel(by calling Schema::closeRel(targetrel))
        //     delete targetrel (by calling Schema::deleteRel(targetrel))
        //     return ret;
        // }

        if(ret != SUCCESS){
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
        
    }

    // std::cout<<BlockBuffer::compareCount<<std::endl;

    Schema::closeRel(targetRel);

    return SUCCESS;
   
}


int Algebra::insert(char relName[ATTR_SIZE],int nAttrs,char record[][ATTR_SIZE]){

    if(strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME) == 0)return E_NOTPERMITTED;

    // get the relation's rel-id using OpenRelTable::gettargetRelId() method
    int targetRelId = OpenRelTable::getRelId(relName);

    // if relation is not open in open relation table, return E_RELNOTOPEN
    // (check if the value returned from gettargetRelId function call = E_RELNOTOPEN)

    if(targetRelId == E_RELNOTOPEN)return targetRelId;

    // get the relation catalog entry from relation cache
    // (use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(targetRelId,&relCatBuf);

    /* if relCatEntry.numAttrs != numberOfAttributes in relation,
       return E_NATTRMISMATCH */
    if(relCatBuf.numAttrs != nAttrs)return E_NATTRMISMATCH;

    // let recordValues[numberOfAttributes] be an array of type union Attribute
    Attribute recordValues[nAttrs];

    /*
        Converting 2D char array of record values to Attribute array recordValues
    */

   // iterate through 0 to nAttrs-1: (let i be the iterator)
   for(int i=0;i<nAttrs;i++){

        // get the attr-cat entry for the i'th attribute from the attr-cache
        // (use AttrCacheTable::getAttrCatEntry())

        AttrCatEntry attrCatEntry;

        AttrCacheTable::getAttrCatEntry(targetRelId,i,&attrCatEntry);

        int type = attrCatEntry.attrType;

        if (type == NUMBER){
            // if the char array record[i] can be converted to a number
            // (check this using isNumber() function)
            if(isNumber(record[i])){

                /* convert the char array to numeral and store it
                   at recordValues[i].nVal using atof() */
                   recordValues[i].nVal = atof(record[i]);
            }else{
                return E_ATTRTYPEMISMATCH;
            }

        }else if (type == STRING){
            // copy record[i] to recordValues[i].sVal
            strcpy(recordValues[i].sVal,record[i]);
        }
    }

    // insert the record by calling BlockAccess::insert() function
    // let retVal denote the return value of insert call

    int retVal = BlockAccess::insert(targetRelId,recordValues);

    // std::cout<<"we did it bro"<<std::endl;

    return retVal;



}


int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {

    int srctargetRelId = OpenRelTable::getRelId(srcRel); /*srcRel's rel-id (use OpenRelTable::gettargetRelId() function)*/

    // if srcRel is not open in open relation table, return E_RELNOTOPEN

    if(srctargetRelId == E_RELNOTOPEN)return E_RELNOTOPEN;

    // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()

    RelCatEntry relCatEntry;

    RelCacheTable::getRelCatEntry(srctargetRelId,&relCatEntry);

    // get the no. of attributes present in relation from the fetched RelCatEntry.

    int numAttrs = relCatEntry.numAttrs;

    // attrNames and attrTypes will be used to store the attribute names
    // and types of the source relation respectively

    char attrNames[numAttrs][ATTR_SIZE];
    int attrTypes[numAttrs];

    /*iterate through every attribute of the source relation :
        - get the AttributeCat entry of the attribute with offset.
          (using AttrCacheTable::getAttrCatEntry())
        - fill the arrays `targetattrNames` and `attrTypes` that we declared earlier
          with the data about each attribute
    */

   for(int i=0;i<numAttrs;i++){

        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srctargetRelId,i,&attrCatEntry);
        strcpy(attrNames[i],attrCatEntry.attrName);
        attrTypes[i] = attrCatEntry.attrType;

    }


    /*** Creating and opening the target relation ***/

    // Create a relation for target relation by calling Schema::createRel()

    int ret = Schema::createRel(targetRel,numAttrs,attrNames,attrTypes);

    // if the createRel returns an error code, then return that value.

    if(ret != SUCCESS) return ret;

    // Open the newly created target relation by calling OpenRelTable::openRel()
    // and get the target targetRelId

    int targettargetRelId = OpenRelTable::openRel(targetRel);

    // If opening fails, delete the target relation by calling Schema::deleteRel() of
    // return the error value returned from openRel().

     if(targettargetRelId < 0 || targettargetRelId > MAX_OPEN ){
        Schema::deleteRel(targetRel);
        return targettargetRelId;
    }

    /*** Inserting projected records into the target relation ***/

    // Take care to reset the searchIndex before calling the project function
    // using RelCacheTable::resetSearchIndex()

    RelCacheTable::resetSearchIndex(srctargetRelId);

    Attribute record[numAttrs];


    while (BlockAccess::project(srctargetRelId,record) == SUCCESS/* BlockAccess::project(srctargetRelId, record) returns SUCCESS */){
        // record will contain the next record

        // ret = BlockAccess::insert(targettargetRelId, proj_record);

        ret = BlockAccess::insert(targettargetRelId,record);

        if (ret != SUCCESS/* insert fails */) {

            // close the targetrel by calling Schema::closeRel()
            // delete targetrel by calling Schema::deleteRel()
            // return ret;
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
            
        }
    }

    // Close the targetRel by calling Schema::closeRel()

    Schema::closeRel(targetRel);

    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_attrs[][ATTR_SIZE]) {

    int srctargetRelId = OpenRelTable::getRelId(srcRel); /*srcRel's rel-id (use OpenRelTable::gettargetRelId() function)*/

    // if srcRel is not open in open relation table, return E_RELNOTOPEN

    if (srctargetRelId < 0 || srctargetRelId >= MAX_OPEN) return E_RELNOTOPEN;

    // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()

    RelCatEntry relCatEntry;

    RelCacheTable::getRelCatEntry(srctargetRelId,&relCatEntry);

    // get the no. of attributes present in relation from the fetched RelCatEntry.

    int src_nAttrs = relCatEntry.numAttrs;

    // declare attr_offset[tar_nAttrs] an array of type int.
    // where i-th entry will store the offset in a record of srcRel for the
    // i-th attribute in the target relation.

    int attr_offset[tar_nAttrs];

    // let attr_types[tar_nAttrs] be an array of type int.
    // where i-th entry will store the type of the i-th attribute in the
    // target relation.

    int attr_types[tar_nAttrs];


    /*** Checking if attributes of target are present in the source relation
         and storing its offsets and types ***/

    /*iterate through 0 to tar_nAttrs-1 :
        - get the attribute catalog entry of the attribute with name tar_attrs[i].
        - if the attribute is not found return E_ATTRNOTEXIST
        - fill the attr_offset, attr_types arrays of target relation from the
          corresponding attribute catalog entries of source relation
    */

    for(int i=0;i<tar_nAttrs;i++){
        AttrCatEntry attrCatEntry;

        int ret = AttrCacheTable::getAttrCatEntry(srctargetRelId,tar_attrs[i],&attrCatEntry);

        if(ret!=SUCCESS) return ret;

        attr_offset[i] = attrCatEntry.offset;
        attr_types[i] = attrCatEntry.attrType;
    }


    /*** Creating and opening the target relation ***/

    // Create a relation for target relation by calling Schema::createRel()

    int ret = Schema::createRel(targetRel,tar_nAttrs,tar_attrs,attr_types);

    // if the createRel returns an error code, then return that value.

    if(ret != SUCCESS)return ret;

    // Open the newly created target relation by calling OpenRelTable::openRel()
    // and get the target targetRelId

    int targettargetRelId = OpenRelTable::openRel(targetRel);

    // If opening fails, delete the target relation by calling Schema::deleteRel()
    // and return the error value from openRel()

    if(targettargetRelId < 0 || targettargetRelId >= MAX_OPEN){
        Schema::deleteRel(targetRel);
        return targettargetRelId;
    }


    /*** Inserting projected records into the target relation ***/

    // Take care to reset the searchIndex before calling the project function
    // using RelCacheTable::resetSearchIndex()


    RelCacheTable::resetSearchIndex(srctargetRelId);
    int j=0;
    Attribute record[src_nAttrs];

    while (BlockAccess::project(srctargetRelId,record) == SUCCESS/* BlockAccess::project(srctargetRelId, record) returns SUCCESS */) {
        // the variable `record` will contain the next record

        // std::cout<<j++<<std::endl;

        Attribute proj_record[tar_nAttrs];

        //iterate through 0 to tar_attrs-1:
        //    proj_record[attr_iter] = record[attr_offset[attr_iter]]

        // ret = BlockAccess::insert(targettargetRelId, proj_record);

        for(int i=0;i<tar_nAttrs;i++){
            proj_record[i] = record[attr_offset[i]];
        }
        
        ret = BlockAccess::insert(targettargetRelId,proj_record);
        if (ret != SUCCESS /* insert fails */) {
            // close the targetrel by calling Schema::closeRel()
            // delete targetrel by calling Schema::deleteRel()
            // return ret;

            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // Close the targetRel by calling Schema::closeRel()

    Schema:: closeRel(targetRel);

    return SUCCESS;
}

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE]) {

    // get the srcRelation1's rel-id using OpenRelTable::gettargetRelId() method

    int srcRelId1 = OpenRelTable::getRelId(srcRelation1);

    int srcRelId2 = OpenRelTable::getRelId(srcRelation2);

    // get the srcRelation2's rel-id using OpenRelTable::gettargetRelId() method

    if( srcRelId1 == E_RELNOTOPEN || srcRelId2 == E_RELNOTOPEN) return E_RELNOTOPEN; 

    // if either of the two source relations is not open
    //     return E_RELNOTOPEN

    AttrCatEntry attrCatEntry1, attrCatEntry2;
    // get the attribute catalog entries for the following from the attribute cache
    // (using AttrCacheTable::getAttrCatEntry())
    // - attrCatEntry1 = attribute1 of srcRelation1
    // - attrCatEntry2 = attribute2 of srcRelation2

    int ret1 = AttrCacheTable::getAttrCatEntry(srcRelId1,attribute1,&attrCatEntry1);

    int ret2 = AttrCacheTable::getAttrCatEntry(srcRelId2,attribute2,&attrCatEntry2);

    // if attribute1 is not present in srcRelation1 or attribute2 is not
    // present in srcRelation2 (getAttrCatEntry() returned E_ATTRNOTEXIST)
    //     return E_ATTRNOTEXIST.

    if(ret1 != SUCCESS) {
        std::cout<<"518: "<<ret1<<"\n";
        return ret1;
    }

    if(ret2 != SUCCESS) {
        std::cout<<"524: "<<ret2<<"\n";
        return ret2;
    }

    // if attribute1 and attribute2 are of different types return E_ATTRTYPEMISMATCH

    if(attrCatEntry1.attrType != attrCatEntry2.attrType)return E_ATTRTYPEMISMATCH;

    // iterate through all the attributes in both the source relations and check if
    // there are any other pair of attributes other than join attributes
    // (i.e. attribute1 and attribute2) with duplicate names in srcRelation1 and
    // srcRelation2 (use AttrCacheTable::getAttrCatEntry())
    // If yes, return E_DUPLICATEATTR

    RelCatEntry relCatEntry1,relCatEntry2;

    RelCacheTable::getRelCatEntry(srcRelId1,&relCatEntry1);
    RelCacheTable::getRelCatEntry(srcRelId2,&relCatEntry2);

    std::cout<<"-----1-----\n";

    for(int attrOffset1=0;attrOffset1<relCatEntry1.numAttrs;attrOffset1++){

        AttrCatEntry temp1;
        AttrCacheTable::getAttrCatEntry(srcRelId1,attrOffset1,&temp1);

        for(int attrOffset2=0;attrOffset2<relCatEntry2.numAttrs;attrOffset2++){

            if(attrOffset1==attrCatEntry1.offset && attrOffset2 == attrCatEntry2.offset)continue;
            
            AttrCatEntry temp2;
            AttrCacheTable::getAttrCatEntry(srcRelId2,attrOffset2,&temp2);
            
            if(strcmp(temp1.attrName,temp2.attrName) == 0){
                return E_DUPLICATEATTR;
            }

        }
    }

    std::cout<<"-----2-----\n";

    // get the relation catalog entries for the relations from the relation cache
    // (use RelCacheTable::getRelCatEntry() function)

    int numOfAttributes1 = relCatEntry1.numAttrs /* number of attributes in srcRelation1 */;
    int numOfAttributes2 = relCatEntry2.numAttrs /* number of attributes in srcRelation2 */;

    // if rel2 does not have an index on attr2
    //     create it using BPlusTree:bPlusCreate()
    //     if call fails, return the appropriate error code
    //     (if your implementation is correct, the only error code that will
    //      be returned here is E_DISKFULL)

    if(attrCatEntry2.rootBlock == -1){
        int ret = BPlusTree::bPlusCreate(srcRelId2,attribute2);

        if(ret != SUCCESS) return ret;
    }

    std::cout<<"-----3-----\n";

    int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;
    // Note: The target relation has number of attributes one less than
    // nAttrs1+nAttrs2 (Why?) ---> because we are adding the tuples according to attr1 = attr2 so no need of attr2

    // declare the following arrays to store the details of the target relation
    char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
    int targetRelAttrTypes[numOfAttributesInTarget];

    // iterate through all the attributes in both the source relations and
    // update targetRelAttrNames[],targetRelAttrTypes[] arrays excluding attribute2
    // in srcRelation2 (use AttrCacheTable::getAttrCatEntry())

    int index = 0;

    for(int i=0;i<numOfAttributes1;i++){
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(srcRelId1,i,&attrCatBuf);
        strcpy(targetRelAttrNames[index],attrCatBuf.attrName);
        targetRelAttrTypes[index] = attrCatBuf.attrType;
        index++;
    }

    std::cout<<"-----4-----\n";

    for(int i=0;i<numOfAttributes2;i++){
        if(i==attrCatEntry2.offset) continue;
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(srcRelId2,i,&attrCatBuf);
        strcpy(targetRelAttrNames[index],attrCatBuf.attrName);
        targetRelAttrTypes[index] = attrCatBuf.attrType;
        index++;
    }

    std::cout<<"-----5-----\n";

    // create the target relation using the Schema::createRel() function

    int ret = Schema::createRel(targetRelation,numOfAttributesInTarget,targetRelAttrNames,targetRelAttrTypes);

    std::cout<<"-----6-----\n";
    
    // if createRel() returns an error, return that error

    if(ret != SUCCESS)return ret;

    // Open the targetRelation using OpenRelTable::openRel()

    int targetRelId = OpenRelTable::openRel(targetRelation);

    std::cout<<"targetRelId is = "<<targetRelId<<"\n";

    std::cout<<"-----7-----\n";
    
    if(targetRelId < 0 || targetRelId >= MAX_OPEN){ // if openRel() fails (No free entries left in the Open Relation Table)
        // delete target relation by calling Schema::deleteRel()
        int ret = Schema::deleteRel(targetRelation);
        return targetRelId;
        // return the error code
    }

    Attribute record1[numOfAttributes1];
    Attribute record2[numOfAttributes2];
    Attribute targetRecord[numOfAttributesInTarget];

    int count = 0;

    RelCacheTable::resetSearchIndex(srcRelId1);
    AttrCacheTable::resetSearchIndex(srcRelId1,attribute1);

    // this loop is to get every record of the srcRelation1 one by one
    while (BlockAccess::project(srcRelId1, record1) == SUCCESS) {

        std::cout<<"projecting....\n";

        // reset the search index of `srcRelation2` in the relation cache
        // using RelCacheTable::resetSearchIndex()

        RelCacheTable::resetSearchIndex(srcRelId2);

        // reset the search index of `attribute2` in the attribute cache
        // using AttrCacheTable::resetSearchIndex()

        AttrCacheTable::resetSearchIndex(srcRelId2,attribute2);

        // this loop is to get every record of the srcRelation2 which satisfies
        //the following condition:
        // record1.attribute1 = record2.attribute2 (i.e. Equi-Join condition)
        while (BlockAccess::search(srcRelId2, record2, attribute2, record1[attrCatEntry1.offset], EQ) == SUCCESS ) {

            // copy srcRelation1's and srcRelation2's attribute values(except
            // for attribute2 in rel2) from record1 and record2 to targetRecord
            int index = 0;
            for(int i=0;i<numOfAttributes1;i++){
                targetRecord[index] = record1[i];
                index++;
            }

            for(int i=0;i<numOfAttributes2;i++){
                if(i==attrCatEntry2.offset) continue;
                targetRecord[index] = record2[i];
                index++;
            }

            // insert the current record into the target relation by calling
            // BlockAccess::insert()

            int ret = BlockAccess::insert(targetRelId,targetRecord);

            if(ret != SUCCESS/* insert fails (insert should fail only due to DISK being FULL) */) {

                // close the target relation by calling OpenRelTable::closeRel()
                // delete targetRelation (by calling Schema::deleteRel())
                OpenRelTable::closeRel(targetRelId);
                Schema::deleteRel(targetRelation);
                return E_DISKFULL;
            }

            std::cout<<count<<" elements inserted\n";
        }
    }

    std::cout<<"-----8-----\n";

    // close the target relation by calling OpenRelTable::closeRel()
    OpenRelTable::closeRel(targetRelId);
    return SUCCESS;
}


