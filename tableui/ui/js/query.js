function updateCheckboxLabels (numColsEmpty) {
  if (!getConfig.config) return

  // Update 'Show empty columns' label
  if (numColsEmpty === undefined) {
    numColsEmpty = emptyColumns(true).length
  }
  const emptyS = numColsEmpty === 1 ? '' : 's'
  $('#showEmptyColumns').siblings('span')
    .text(`Show ${numColsEmpty} empty column${emptyS}`)

  // Count columns hidden by configuration
  const columnOptionsMap = array2object(getConfig.config.dataTablesAdditions.columnOptions || [], 'name')
  let nHidden = 0
  for (const name of Object.keys(columnOptionsMap)) {
    if (columnOptionsMap[name].visible === false) nHidden++
  }
  const hiddenS = nHidden === 1 ? '' : 's'
  $('#showHiddenColumns').siblings('span')
    .text(`Show ${nHidden} column${hiddenS} hidden by configuration`)
}

function colsShowAdd (flag) {
  // Add a flag to _cols_show, preserving other flags. Removes 'all' if present.
  const current = getQueryValue('_cols_show') || ''
  const flags = current.split(',').map(s => s.trim()).filter(f => f && f !== 'all')
  if (!flags.includes(flag)) flags.push(flag)
  setQueryValue('_cols_show', flags.length ? flags.join(',') : null)
}

function colsShowRemove (flag) {
  // Remove a flag from _cols_show, preserving other flags.
  const current = getQueryValue('_cols_show') || ''
  const flags = current.split(',').map(s => s.trim()).filter(f => f && f !== 'all' && f !== flag)
  setQueryValue('_cols_show', flags.length ? flags.join(',') : null)
}

function colsShowHas (flag) {
  const current = getQueryValue('_cols_show') || ''
  return current.split(',').map(s => s.trim()).includes(flag)
}

function setQueryLink (url) {
  // Store URL for use in adjustDOM() after table draw.
  setQueryLink.url = url
}

function parseQueryString (component, hash) {
  // http://paulgueller.com/2011/04/26/parse-the-querystring-with-jquery/
  const nvpair = {}
  let qs = window.location.hash.replace('#', '')
  if (hash) {
    qs = hash.replace('#', '')
  }
  if (qs.length === 0) {
    return {}
  }
  const pairs = qs.split('&')
  $.each(pairs, function (i, v) {
    const pair = v.split('=')
    if (component === 'search' && pair[0].startsWith('_')) {
      return // Skip state parameters
    }
    if (component === 'state' && !pair[0].startsWith('_')) {
      return // Keep state parameters
    }
    nvpair[pair[0]] = pair[1]
  })

  return nvpair
}

function setDefaultQueryString (hash) {
  const currentHash = window.location.hash.replace('#', '')
  if (hash && !currentHash) {
    const qs = parseQueryString()
    const qsDefault = parseQueryString(null, hash)
    for (const [key, val] of Object.entries(qsDefault)) {
      if (!(key in qs)) {
        let msg = 'init() => Setting query string parameter '
        msg += `${key} = ${val} from defaultHash: `
        console.log(msg)
        setQueryValue(key, val)
      }
    }
  }
}

function getQueryValue (name, defaultValue) {
  const qs = parseQueryString()
  if (!qs[name]) {
    return defaultValue
  }
  return qs[name]
}

function setQueryValue (name, val) {
  console.log(`setQueryValue() called with name='${name}' and val='${val}'`)
  const qs = parseQueryString()
  if (val === null) {
    console.log(`setQueryValue() => Removing ${name} from query string.`)
    delete qs[name]
  } else {
    qs[name] = val
  }

  // Put _ parameters at end of query string
  const sortedKeys = Object.keys(qs).sort((a, b) => {
    if (a.startsWith('_')) return 1
    return -1
  })

  const sortedQs = {}
  for (const key of sortedKeys) {
    sortedQs[key] = qs[key]
  }
  window.location.hash = decodeURIComponent($.param(sortedQs))
}

function checkQueryString (config) {
  console.log('checkQueryString() => Checking query string for invalid column names.')

  const _colsShow = getQueryValue('_cols_show')
  if (_colsShow && _colsShow === 'all') {
    setQueryValue('_cols_show', null)
  }

  const qs = parseQueryString()
  console.log('checkQueryString() => Query string:')
  console.log(qs)
  const columnObject = array2object(config.dataTables.columns, 'name')

  const msg = "checkQueryString() => what = 'keys'. Checking keys but not "
  console.log(`${msg}values in query string.`)
  let alerted = false
  for (const key of Object.keys(qs)) {
    if (key.startsWith('_')) {
      const msg = `checkQueryString() => found state parameter '${key}' in `
      console.log(`${msg}query string. Leaving it.`)
      continue
    }
    if (key in columnObject) {
      console.log(`checkQueryString() => Found valid key = '${key}' in query string.`)
    } else {
      console.log(`checkQueryString() => Found invalid key = '${key}' in query string. Removing it.`)
      if (alerted === false) {
        alerted = true
        let amsg = `Invalid column name in query string: "${key}". `
        amsg += 'Removing it from query string and any other invalid column names.'
        window.alert(amsg)
      }
      setQueryValue(key, null)
    }
  }

  console.log("checkQueryString() => what = 'cols'. Checking _cols and _cols_regex parameters in query string.")
  let _cols = getQueryValue('_cols')
  let _colsRegex = getQueryValue('_cols_regex')
  if (!_cols && !_colsRegex) {
    console.log('checkQueryString() => No _cols or _cols_regex in query string. Leaving them.')
    return
  }

  if (_cols) {
    _cols = _cols.split(',')
    const validCols = []
    let updateCols = false
    for (let i = 0; i < _cols.length; i++) {
      const columnName = _cols[i]
      if (columnName in columnObject) {
        validCols.push(columnName)
        continue
      }

      const msg = `checkQueryString() => Column name '${columnName}' not `
      console.log(`${msg}found in column names. Removing it from _cols.`)
      if (!updateCols) {
        let amsg = 'checkQueryString() => Column name in query string not '
        amsg += `found: "${columnName}". Removing it and any other invalid `
        window.alert(`${amsg}column names from query string.`)
      }
      updateCols = true
    }

    if (updateCols) {
      console.log('checkQueryString() => Updating query string to remove invalid _cols values.')
      _cols = validCols.filter(Boolean)
      console.log(_cols)
      setQueryValue('_cols', _cols.length ? _cols.join(',') : null)
    }
  }

  if (_colsRegex) {
    _colsRegex = _colsRegex.split(',')
    const validRegexes = []
    let updateColsRegex = false
    for (let i = 0; i < _colsRegex.length; i++) {
      const columnRegex = _colsRegex[i]
      try {
        new RegExp(columnRegex)
        validRegexes.push(columnRegex)
        continue
      } catch (e) {
        console.log(`checkQueryString() => Invalid _cols_regex regex '${columnRegex}'. Removing it.`)
      }

      if (!updateColsRegex) {
        let amsg = 'checkQueryString() => Invalid regex in _cols_regex: '
        amsg += `"${columnRegex}". Removing it and any other invalid regexes.`
        window.alert(amsg)
      }
      updateColsRegex = true
    }

    if (updateColsRegex) {
      console.log('checkQueryString() => Updating query string to remove invalid _cols_regex values.')
      _colsRegex = validRegexes.filter(Boolean)
      console.log(_colsRegex)
      setQueryValue('_cols_regex', _colsRegex.length ? _colsRegex.join(',') : null)
    }
  }
}

function setQueryStringFromSearch () {
  let msg = 'setQueryStringFromSearch() => Getting query string from search inputs.'
  console.log(`${msg}search inputs.`)
  // Step through column search inputs and update query string
  // Highlight inputs with search values and remove highlight for
  // inputs with no search value.
  let inputs
  if ($('.dtfh-floatingparent').length > 0) {
    msg = 'setQueryStringFromSearch() => .dtfh-floatingparent found. '
    console.log(`${msg}Using inputs under it.`)
    inputs = $('.dtfh-floatingparent input.columnSearch')
  } else {
    msg = 'setQueryStringFromSearch() => No .dtfh-floatingparent found. '
    console.log(`${msg}Using inputs under .dataTables_scrollHead`)
    inputs = $(`.dataTables_scrollHead input.columnSearch`)
  }

  msg = 'setQueryStringFromSearch() => Reading '
  console.log(`${msg}${inputs.length} column search inputs.`)
  const qsSearch = parseQueryString('search')

  for (const input of inputs) {
    const name = $(input).attr('name')
    let searchValue = $(input).val()
    console.log(`setQueryStringFromSearch() =>   {name: '${name}', searchValue: '${searchValue}'}`)
    if (searchValue) {
      console.log(`setQueryStringFromSearch() => Found search value for column '${name}': '${searchValue}'.`)
      let msg = 'setQueryStringFromSearch() => Found search value for column'
      msg += ` '${name}'. Updating query string and highlighting input.`
      $(input).css('background-color', 'yellow')
      msg = 'setQueryStringFromSearch() => Updating query string with '
      msg += `search value for column '${name}' = '${searchValue}'.`
      console.log(msg)
      setQueryValue(name, searchValue)
    } else {
      // console.log(`No search value for column '${name}'.`);
      $(input).css('background-color', '')
      if (qsSearch[name]) {
        const msg = `setQueryStringFromSearch() => Found ${name} in `
        console.log(`${msg}query string and empty input value. Removing ${name} from query string.`)
        setQueryValue(name, null)
      }
    }
  }

  const qs = parseQueryString('search')
  const numSearchKeys = Object.keys(qs).length
  msg = 'setQueryStringFromSearch() => There are '
  if (numSearchKeys > 0) {
    console.log(`${msg}${numSearchKeys} search keys in the query string. Showing Clear button.`)
    $('#clearAllSearches').show()
  } else {
    console.log(`${msg}no search terms in the query string. Hiding Clear button.`)
    $('#clearAllSearches').hide()
  }

  const globalSearch = $(tableID).DataTable().search()
  if (globalSearch) {
    setQueryValue('_globalsearch', encodeURIComponent(globalSearch))
    $('#clearAllSearches').show()
    const historyKey = 'globalSearchHistory:' + window.location.pathname
    const history = JSON.parse(localStorage.getItem(historyKey) || '[]')
    if (!history.includes(globalSearch)) {
      history.unshift(globalSearch)
      if (history.length > 20) history.pop()
      localStorage.setItem(historyKey, JSON.stringify(history))
    }
  } else {
    setQueryValue('_globalsearch', null)
  }
}
