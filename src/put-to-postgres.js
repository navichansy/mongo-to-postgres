/**
   * Insert data to destination table
   * @param {object} kenx - knex object
   * @param {Array} collections - Array of collections
   * @param {string} tableName - Table name
   * @param {string} rows - Objects to insert
   * @return {Array} Ids map
   */
export default async ({ knex, collections, tableName, rows }) => {
  const { foreignKeys, fieldsRename, fieldsRedefine, links, jsonFileName, ignoreField } =
    collections.find(c => c.tableName === tableName);

  const idsMap = []; // array for identifiers maps
  for (const currentRow of rows) {
    // rename fields (if necessary)
    if (fieldsRename) {
      for (const value of Object.values(fieldsRename)) {
        if (value[1]) {
          currentRow[value[1]] = currentRow[value[0]];
        }
        delete currentRow[value[0]];
      }
    }

    // redefine attributes
    if (fieldsRedefine) {
      for (const field of fieldsRedefine) {
        currentRow[field[0]] = field[1];
      }
    }

    // map foreign keys
    if (foreignKeys) {
      for (const [fieldName, collectionName] of Object.entries(foreignKeys)) {
        const foreignCollection = collections.find(c => c.collectionName === collectionName);
        const maps = foreignCollection.idsMap;
        if (!Array.isArray(currentRow[fieldName])) {
          const mapedField = maps
            .find(x => x.oldId === (currentRow[fieldName] ? currentRow[fieldName].toString() : null));
          currentRow[fieldName] = mapedField
            ? currentRow[fieldName] = mapedField.newId
            : currentRow[fieldName] = null;
        }
      }
    }

    // save and then delete Mongo _id
    // if (currentRow) console.log(currentRow);
    const oldId = currentRow._id.toString();
    delete currentRow._id;

    // remove arrays from row object

    const rowCopy = JSON.parse(JSON.stringify(currentRow));
    // console.log('rowCopy before', rowCopy);
    for (const fieldName of Object.keys(rowCopy)) {
      const toIgnore = ignoreField && ignoreField.includes(fieldName)
      if (Array.isArray(rowCopy[fieldName])) {
        const isJsonField = jsonFileName && jsonFileName.find(j => j.name === fieldName);
        // if (isJsonField !== null) console.log( isJsonField);
        if (!isJsonField) {
          delete rowCopy[fieldName];
        } else {
          let newList
          if (isJsonField && isJsonField.substitueIdTo) { // only applied to json with array of ids
            const foreignCollection = collections.find(c => c.collectionName === isJsonField.substitueIdTo);
            if (foreignCollection && foreignCollection.idsMap !== undefined) {
              const maps = foreignCollection.idsMap;
              const newArray = rowCopy[fieldName].map(x => maps.find(xx => xx.oldId === x).newId.toString())
              // console.log(newArray)
              newList = JSON.stringify(newArray);
            }
          } else if (isJsonField && isJsonField.foreignKeys) {
            let newObjArray = [];
            //  console.log('fieldName', fieldName);
            //  console.log('isJsonField', isJsonField);
            for (const record of rowCopy[fieldName]) {
              let arrayObject = {};
              //  console.log('record', record);
              // console.log('Object.keys(record)', Object.keys(record));
              for (const keyName of Object.keys(record)) {
                // console.log('keyName', keyName);
                const hasForiegnKey = Object.keys(isJsonField.foreignKeys).find(key => key === keyName);
                if (hasForiegnKey) {
                  //  console.log('hasForiegnKey', hasForiegnKey, isJsonField.foreignKeys[hasForiegnKey]);
                  const foreignCollection = collections.find(c => c.collectionName === isJsonField.foreignKeys[hasForiegnKey]);
                  const maps = foreignCollection ? foreignCollection.idsMap : null;
                  //   console.log('maps', maps.length)
                  const matched = maps ? maps.find(xx => xx.oldId === record[keyName]) : null
                  arrayObject[keyName] = matched
                    ? matched.newId.toString()
                    : record[keyName];
                  //  console.log('arrayObject[keyName]', keyName, arrayObject[keyName])
                } else {
                  //   console.log('keyName', keyName);
                  arrayObject[keyName] = record[keyName];
                }
              }
              newObjArray.push(arrayObject);
            }
            newList = JSON.stringify(newObjArray)
          } else {
            newList = JSON.stringify(rowCopy[fieldName]);
          }
          rowCopy[fieldName] = newList;
        }
      } else if (fieldName === 'id') {  //remove 'id' field in original mongoDB
        delete rowCopy[fieldName];
      }
      if (toIgnore) {  //remove ignore field in original mongoDB
        delete rowCopy[fieldName];
      }
    }
    //console.log('rowCopy', rowCopy)
    // insert current row
    const newId = await knex(tableName)
      .returning('id')
      .insert(rowCopy);

    // save id mapping
    idsMap.push({ oldId, newId: newId[0] });

    // many-to-many links
    if (links) {
      for (const [fieldName, linksTableAttrs] of Object.entries(links)) {
        for (const relatedField of currentRow[fieldName]) {
          const foreignCollection = collections.find(c => c.collectionName === foreignKeys[fieldName]);
          let foreignKey;
          let linkRow = {};
          // if related field contains just ID
          if (relatedField.constructor.name === 'ObjectID') {
            foreignKey = relatedField.toString();
          } else {
            // or if it contains additional fields
            const func = linksTableAttrs[3];
            const res = func(linkRow, relatedField);
            foreignKey = res.foreignKey;
            linkRow = res.linkRow;
          }
          const map = foreignCollection.idsMap.find(x => x.oldId === foreignKey);
          linkRow[linksTableAttrs[1]] = newId[0];
          linkRow[linksTableAttrs[2]] = map.newId;
          await knex(linksTableAttrs[0]).insert(linkRow);
        }
      }
    }
    // next row
  }

  console.log(`Inserted ${rows.length} rows to "${tableName}" table`);
  return idsMap;
};
