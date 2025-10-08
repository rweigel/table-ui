function renderColumn (columnName, tableConfig) {
  if (columnName === 'datasetID') {
    _renderColumn.columnName = columnName
    _renderColumn.tableConfig = tableConfig

    const tableName = tableConfig.tableUI.tableMetadata.name
    if (!tableName) {
      return
    }
    if (tableName.startsWith('cdaweb') || tableName.startsWith('spase')) {
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
      columnString += `&nbsp;<a href="${fnameCDF}" title="Master CDF" target="_blank">M</a>`
      columnString += `&nbsp;<a href="${fnameJSON}" title="JSON" target="_blank">J</a>`
      columnString += `&nbsp;<a href="${fnameSKT}" title="Skeleton Table" target="_blank">SK</a>`

      const tableName = _renderColumn.tableConfig.tableUI.tableMetadata.name
      if (tableName === 'spase.dataset') {
        console.log(full[1].replace('spase://', 'https://hpde.io'))
        const fnameSPASE = full[1].replace('spase://', 'https://hpde.io/') + '.json'
        columnString += `&nbsp;<a href="${fnameSPASE}" title="SPASE" target="_blank">SP</a>`
      }

      if (tableName === 'cdaweb.dataset') {
        const columnNames = _renderColumn.tableConfig.columns.map(c => c.name)
        console.log(columnNames)
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
  console.log('renderTableMetadata(): config:')

  if (!config.tableUI.tableMetadata) return ''

  let description = config.tableUI.tableMetadata.description || ''
  if (description) {
    description = `<p>${description}<p>`
  }

  let name = config.tableUI.tableMetadata.name || ''
  if (name) {
    name = `<code>${name}</code>`
  }

  let creationDate = config.tableUI.tableMetadata.creationDate || ''
  if (creationDate) {
    creationDate = ` | Created ${creationDate}.`
  }

  const base = window.location.origin + window.location.pathname.replace(/\/$/, '')
  let txt = ` Table ${name}: <a href="${base}/config" title="${base}/config" target="_blank">config</a>`
  let href = ''
  if (config.tableUI.sqldb) {
    href = `${base}/sqldb`
  }
  if (config.tableUI.jsondb) {
    href = `${base}/jsondb`
  }
  txt += ` | <a href="${href}" title="${href}" target="_blank">data</a>`
  txt += creationDate
  txt += description
  return txt
}
