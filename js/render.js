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

function renderTableInfo (config) {
  let html = `This table is draws content from the <code>${config.tableName}</code> table in `
  html += '<a href="http://mag.gmu.edu/git-data/cdawmeta/data/table">the tables directory</a>.'
  if (config.tableName === 'cdaweb.dataset') {
    html += ' This table contains information from <a href="https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml">all.xml</a> and <code>CDFglobalAttributes</code> in the <a href="https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/">Master CDF files</a>.'
  }
  if (config.tableName === 'spase.dataset') {
    html += ' This table contains non-<code>Parameter</code> SPASE <code>NumericalData</code> metadata.'
  }
  if (config.tableName === 'spase.parameter') {
    html += ' This table contains only information in SPASE <code>NumericalData/Parameter</code> metadata.'
  }
  return html
}
