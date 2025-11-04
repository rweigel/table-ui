const renderFunctions = {}

renderFunctions.ellipsis = function (columnName, config, n) {
  return DataTable.render.ellipsis(n || 30)
}
renderFunctions.trimURL = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    const urlSplit = columnString.split('/')
    if (urlSplit[urlSplit.length - 1] !== '') {
      const attrs = `href="${columnString}" title="${columnString}"`
      let urlShort = urlSplit[urlSplit.length - 1]
      urlShort = `<a ${attrs} target="_blank">${urlShort}</a>`
      return urlShort
    }
    return columnString
  }
}
renderFunctions.underline = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    if (columnString) {
      let style = 'text-decoration-line: underline;'
      style += 'text-decoration-style: wavy;'
      return `<div style="${style}">${columnString}</div>`
    }
    return columnString
  }
}

renderFunctions.annotate = function (columnName, config, symbol) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    if (columnString === 'a02') {
      return `${symbol}${columnString}`
    }
    return columnString
  }
}

renderFunctions.bold = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    if (!columnString) {
      return columnString
    }
    return `<span style="font-weight:bold">${columnString}</span>`
  }
}
