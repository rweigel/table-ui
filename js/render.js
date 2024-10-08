function renderColumn (columnName, tableConfig) {
  if (columnName === 'datasetID') {
    _renderColumn.columnName = columnName
    _renderColumn.tableConfig = tableConfig
    if (tableConfig.tableName.startsWith('cdaweb')) {
      return _renderColumn
    }
    if (tableConfig.tableName.startsWith('spase')) {
      return _renderColumn
    }
  }

  function _renderColumn (columnString, type, full, meta) {
    if (type === 'display') {
      //console.log(columnString, type, full, meta)
      //console.log(renderColumn.tableConfig)
      // TODO: Not all have "_v01".
      const base = 'https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/'
      const fnameCDF = base + '0MASTERS/' + columnString.toLowerCase() + '_00000000_v01.cdf'
      const fnameJSON = base + '0JSONS/' + columnString.toLowerCase() + '_00000000_v01.json'
      const fnameSKT = base + '0SKELTABLES/' + columnString.toLowerCase() + '_00000000_v01.skt'
      columnString = `${columnString}`
      columnString += `&nbsp;<a href="${fnameCDF}" title="Master CDF">M</a>`
      columnString += `&nbsp;<a href="${fnameJSON}" title="JSON">J</a>`
      columnString += `&nbsp;<a href="${fnameSKT}" title="Skeleton Table">SK</a>`

      const tableName = _renderColumn.tableConfig.tableName
      if (tableName === 'spase.dataset') {
        console.log(full[1].replace('spase://', 'https://hpde.io'))
        const fnameSPASE = full[1].replace('spase://', 'https://hpde.io/') + '.json'
        columnString += `&nbsp;<a href="${fnameSPASE}" title="SPASE">SP</a>`
      }

      if (tableName === 'cdaweb.dataset') {
        const columnNames = _renderColumn.tableConfig.columnNames
        const index = columnNames.indexOf('spase_DatasetResourceID')
        const fnameSPASE = full[index].replace('spase://', 'https://hpde.io/') + '.json'
        columnString += `&nbsp;<a href="${fnameSPASE}" title="SPASE">SP</a>`
      }
      return columnString
    }
    return columnString
  }
}

function renderTableMetadata (config) {
  const description = config.tableMetadata.description
  const creationDate = config.tableMetadata.creationDate
  return `${description} Table created <code>${creationDate}</code>`
}
