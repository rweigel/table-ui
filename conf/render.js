const renderFunctions = {}

renderFunctions.ellipsis = function (columnName, config, n) {
  return DataTable.render.ellipsis(n || 30)
}

renderFunctions.underline = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    if (columnString) {
      const style = 'text-decoration-line: underline; text-decoration-style: wavy;'
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

    return `<span style="font-weight:bold">${columnString}</span>`
  }
}
