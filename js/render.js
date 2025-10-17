function renderColumn (columnName, config) {
  const columnOptionsArray = config.dataTablesAdditions.columnOptions || null
  if (!columnOptionsArray) return null

  // Create a map from column name to index
  const columnOptions = {}
  for (let i = 0; i < columnOptionsArray.length; i++) {
    columnOptions[columnOptionsArray[i].name] = columnOptionsArray[i]
  }

  let functionName = columnOptions[columnName]?.render

  if (typeof functionName === 'string') {
    return functions[functionName](columnName, config)
  } else if (typeof functionName === 'object') {
    functionName = functionName.function
    const args = columnOptions[columnName].render?.args || []
    return functions[functionName](columnName, config, ...args)
  }
}

const functions = {}

functions.ellipsis = function (columnName, config, n) {
  return DataTable.render.ellipsis(n || 30)
}

functions.renderDatasetID = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }

    const columnNames = config.dataTables.columns.map(c => c.name)

    // TODO: Not all have "_v01".
    const base = 'https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/'
    const fnameCDF = base + '0MASTERS/' + columnString.toLowerCase() + '_00000000_v01.cdf'
    const fnameJSON = base + '0JSONS/' + columnString.toLowerCase() + '_00000000_v01.json'
    const fnameSKT = base + '0SKELTABLES/' + columnString.toLowerCase() + '_00000000_v01.skt'
    const fnameHAPI1 = `https://cdaweb.gsfc.nasa.gov/hapi/info?id=${columnString}`
    const fnameHAPI2 = `https://cottagesystems.com/server/cdaweb/hapi/info?id=${columnString}`

    columnString = `${columnString}`
    columnString += '<span style="font-size:0.75em">'
    columnString += ` <a href="${fnameCDF}"   title="Master CDF" target="_blank">M</a>`
    columnString += ` <a href="${fnameJSON}"  title="Master JSON" target="_blank">J</a>`
    columnString += ` <a href="${fnameSKT}"   title="Master Skeleton Table" target="_blank">SK</a>`

    const tableName = config.dataTablesAdditions.tableMetadata.tableName
    if (tableName === 'cdaweb.variable') {
      const index = columnNames.indexOf('VAR_TYPE')
      if (row[index] === 'data') {
        columnString += ` <a href="${fnameHAPI1}" title="HAPI Info" target="_blank">H<sub>1</sub></a>`
        columnString += ` <a href="${fnameHAPI2}" title="HAPI Info Dev Server" target="_blank">H<sub>2</sub></a>`
      }
    }

    if (tableName === 'spase.dataset') {
      console.log(row[1].replace('spase://', 'https://spase-group.org/'))
      const fnameSPASE = row[1].replace('spase://', 'https://spase-group.org/') + '.json'
      columnString += ` <a href="${fnameSPASE}" title="SPASE" target="_blank">SP</a>`
    }

    if (tableName === 'cdaweb.dataset') {
      const index = columnNames.indexOf('spase_DatasetResourceID')
      const fnameSPASE = row[index].replace('spase://', 'https://hpde.io/') + '.json'
      columnString += `&nbsp;<a href="${fnameSPASE}" title="SPASE">SP</a>`
    }

    columnString += '</span>'

    return columnString
  }
}

function renderTableMetadata (config) {
  console.log('renderTableMetadata(): config:')

  if (!config.dataTablesAdditions.tableMetadata) return ''

  let description = config.dataTablesAdditions.tableMetadata.description || ''
  if (description) {
    description = `<p>${description}<p>`
  }
  const descriptionPlus = config.dataTablesAdditions.tableMetadata['description+'] || ''
  if (descriptionPlus) {
    description += `<p>${descriptionPlus}<p>`
  }

  let tableName = config.dataTablesAdditions.tableMetadata.tableName || ''
  if (tableName) {
    tableName = ` <code>${tableName}</code>`
  }

  let creationDate = config.dataTablesAdditions.tableMetadata.creationDate || ''
  if (creationDate) {
    creationDate = ` | Created ${creationDate}.`
  }

  // const base = window.location.origin + window.location.pathname.replace(/\/$/, '')
  let txt = ` Table${tableName}: `
  txt += '<a href="config" title="config" target="_blank">config</a>'

  const href = config.dataTablesAdditions.file
  txt += ` | <a href="${href}" title="${href}" target="_blank">data</a>`

  txt += creationDate
  txt += description

  return txt
}
