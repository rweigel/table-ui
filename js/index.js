const tableID = '#table1'
console.log('Document ready. Calling init()')
$(document).ready(() => init(true))

async function init (firstLoad) {
  if (DataTable.isDataTable(tableID)) {
    let msg = `init() => isDataTable('${tableID}') is true. Destroying `
    msg += 'existing table before re-creating it.'
    console.log(msg)
    const state = $(tableID).DataTable().state()
    console.log('init() => Current DataTable state:', state)
    destroy(tableID)
  }

  const config = await getConfig()

  if (firstLoad) {
    setDefaultQueryString(config.dataTablesAdditions.defaultQueryString)
  }

  checkQueryString(config)

  createRelatedTablesDropdown(config)

  // Uses renderTableMetadata() if defined in render.js
  if (window.renderTableMetadata) {
    $('#tableMetadata').html(window.renderTableMetadata(config))
  }

  $('title').text(config.dataTablesAdditions.tableMetadata.tableName)

  // Add header names to table. Two rows are added. The first row is for
  // name and sorting, second row is for filtering. Need to add before
  // table is created so it is included in DataTables column width
  // calculations.
  $(tableID).append('<thead><tr></tr><tr></tr></thead>')
  const tr0 = $(`${tableID} > thead > tr:eq(0)`)
  const tr1 = $(`${tableID} > thead > tr:eq(1)`)
  for (let i = 0; i < config.dataTables.columns.length; i++) {
    const name = config.dataTables.columns[i].name
    tr0.append(`<th name="${name}"></th>`)
    tr1.append(`<th name="${name}">${name}</th>`)
  }

  setColumnWidths(config.dataTablesAdditions.columnOptions)

  // https://datatables.net/reference/option/
  const dataTableOptions =
    {
      ...config.dataTables,
      autoWidth: true,
      stateSave: true,
      stateSaveCallBack: function (settings, data) {
      },
      stateLoadParams: function (settings, data) {
        // displayStart and pageLength in top-level config above are
        // ignored if stateSave is true because saved state has
        // precedence over this setting. Setting the state above
        // is needed because after init() is called a second time,
        // stateLoadParams is not triggered. destroy() must delete
        // state because its column visibility will be used instead
        // of the new column visibility.
        let msg = 'stateLoadParams() called. Deleting start, length, and '
        msg += `columns from saved state data: ${data}`
        console.log(msg)
        delete data.start
        delete data.length
        delete data.columns
      },
      headerCallback: function (thead, data, start, end, display) {
        // This is needed to prevent DataTables from removing the
        // second header row that contains the column names. It does
        // not appear to be documented.
        return thead
      },
      initComplete: dtInitComplete
    }

  console.log('init() => Calling DataTable() with options:')
  // dataTableOptions copy is needed b/c DataTable() modifies it
  // and console.log() shows the modified version.
  console.log(JSON.parse(JSON.stringify(dataTableOptions)))
  const table = $(tableID).DataTable(dataTableOptions)
  console.log(`DataTable() returned: ${table}`)
}

function dtInitComplete () {
  console.log('dtInitComplete() => DOM is ready.')

  fixStickyHeader(101)

  const table = $(tableID).dataTable()
  if (getQueryValue('_cols_show') === 'nonempty') {
    let msg = 'dtInitComplete() => Setting hideEmptyColumns '
    msg += 'checkbox to checked because _cols_show=nonempty in query string.'
    console.log(msg)
    $('#hideEmptyColumns').prop('checked', true)
    console.log('dtInitComplete() => Hiding empty columns.')
    const columnEmpty = emptyColumns(tableID, true)
    table.api().columns(columnEmpty).visible(false, false)
  } else {
    console.log('dtInitComplete() => Showing all columns.')
  }

  setEvents(tableID)

  // Must be before adjustDOM() b/c calls $(window).resize(), which triggers
  // widths of columns to be recalculated based on content added in call
  // to createColumnConstraints().
  createColumnConstraints(tableID)

  adjustDOM(tableID)

  watchForFloatingHeader()

  console.log('dtInitComplete() finished.')
}

async function getConfig () {
  const url = window.location.pathname + 'config'
  let config
  try {
    console.log('getConfig() => Getting config from ' + url)
    const resp = await fetch(url)
    config = await resp.json()
  } catch (e) {
    const emsg = 'An error occurred while getting table configuration'
    const fullURL = window.location.origin + url
    const link = `<a href="${fullURL}">${fullURL}</a>`
    $('#error').html(`${emsg} from ${link}. See console for details.`).show()
    console.error(`getConfig() => Error getting config: ${e}`)
    throw e
  }

  updateConfig(config)
  console.log('getConfig() => Setting getConfig.config = config.')
  getConfig.config = config

  return config

  function updateConfig (config) {
    // Update entries in config.dataTables.columns as needed.
    _columns(config)

    // Update config['pageLength'] and config['lengthMenu'] as needed.
    _pageLength(config)

    const page0based = (-1 + parseInt(getQueryValue('_page', 1)))
    config.dataTables.displayStart = config.dataTables.pageLength * page0based

    config.dataTables.ajax = window.location.pathname + 'data/'

    if (config.dataTables.serverSide) {
      config.dataTables.ajax = {
        url: window.location.pathname + 'data/',
        type: 'get',
        data: _ajaxData,
        error: _ajaxError
      }
    }

    console.log('getConfig() => Returning config:')
    console.log(config)

    return config
  }

  function _pageLength (config) {
    // Update pageLength and lengthMenu if needed.
    if (!config.dataTables.pageLength) {
      config.dataTables.pageLength = 25
      console.log('_pageLength() => No config.dataTables.pageLength. Setting to 25')
      const msgo = '_pageLength() => config.dataTables.lengthMenu[0][0]'
      try {
        config.dataTables.pageLength = config.dataTables.lengthMenu[0][0]
        let msg = `${msgo} found. Modifying default pageLength to be first `
        msg += `entry in config.dataTables.lengthMenu: ${config.dataTables.pageLength}`
        console.log(msg)
      } catch (e) {
        const msg = `${msgo} not found. Could not get modified default `
        console.log(`${msg}pageLength from config.dataTables.lengthMenu. Using 25.`)
      }
    }
    let pageLength = config.dataTables.pageLength
    if (getQueryValue('_length')) {
      let msg = '_pageLength() => _length query value found. Modifying default '
      msg += `pageLength to be _length: ${getQueryValue('_length')}`
      console.log(msg)
      pageLength = parseInt(getQueryValue('_length'))
    }
    config.dataTables.pageLength = pageLength

    if (config.dataTables.lengthMenu) {
      try {
        let msg = '_pageLength() => Updating config.dataTables.lengthMenu'
        msg += ` to include pageLength = ${pageLength}, if not already present.`
        console.log(msg)
        const menuVals = config.dataTables.lengthMenu[0]
        const menuNames = config.dataTables.lengthMenu[1]
        if (!menuVals.includes(pageLength)) {
          menuVals.push(pageLength)
          menuNames.push(pageLength)
          const msg = '_pageLength() => Updated config.dataTables.lengthMenu to '
          console.log(`include ${msg}pageLength = ${pageLength}.`)
          config.dataTables.lengthMenu[0] = menuVals
          config.dataTables.lengthMenu[1] = menuNames
        } else {
          msg = `_pageLength() => pageLength = ${pageLength} already in `
          msg += 'config.dataTables.lengthMenu. Not updating it.'
          console.log(msg)
        }
      } catch (e) {
        let msg = '_pageLength() => Problem with config.dataTables.lengthMenu. '
        msg += `Setting it to [[${pageLength}, -1], [${pageLength}, 'All']].`
        console.log(msg)
        config.dataTables.lengthMenu = [[pageLength, -1], [pageLength, 'All']]
      }
    } else {
      let msg = '_pageLength() => No config.dataTables.lengthMenu. Setting it to '
      msg += `[[${pageLength}, -1], [${pageLength}, 'All']].`
      console.log(msg)
      config.dataTables.lengthMenu = [[pageLength, -1], [pageLength, 'All']]
    }

    // Sort lengthMenu[0] and lengthMenu[1] by lengthMenu[0] values but keep
    // the correspondence between values and names.
    if (config.dataTables.lengthMenu) {
      const msg = '_pageLength() => Sorting config.dataTables.lengthMenu by '
      console.log(`${msg} values in config.dataTables.lengthMenu[0].`)
      const vals = config.dataTables.lengthMenu[0]
      const names = config.dataTables.lengthMenu[1]
      // Create array of [val, name], sort by val, then split back
      const pairs = vals.map((v, i) => [v, names[i]])
      pairs.sort((a, b) => a[0] - b[0])
      config.dataTables.lengthMenu[0] = pairs.map(p => p[0])
      config.dataTables.lengthMenu[1] = pairs.map(p => p[1])

      // If -1 is in lengthMenu[0], put it at the end of the list.
      const minusOneIndex = vals.indexOf(-1)
      if (minusOneIndex !== -1 && minusOneIndex !== vals.length - 1) {
        // Remove -1 and its name
        const [minusOneVal] = vals.splice(minusOneIndex, 1)
        const [minusOneName] = names.splice(minusOneIndex, 1)
        // Push to end
        vals.push(minusOneVal)
        names.push(minusOneName)
      }
      config.dataTables.lengthMenu[0] = vals
      config.dataTables.lengthMenu[1] = names
    }
  }

  function _columns (config) {
    console.log("_columns() => Creating config['columns']")

    let _cols = getQueryValue('_cols')
    console.log('_columns() => _cols from query string:', _cols)
    let allVisible = true
    if (_cols) {
      allVisible = false
    } else {
      _cols = []
    }

    const qs = parseQueryString()
    config.dataTables.searchCols = []
    const columns = config.dataTables.columns
    for (let i = 0; i < columns.length; i++) {
      columns[i].title = columns[i].title || columns[i].name
      // Needed for _verbose server response?
      // columns[i]['data'] = columns[i]['data'] || columns[i]['name'];
      columns[i].target = columns[i].target || i

      const visible = allVisible || _cols.includes(columns[i].name)
      if ([null, undefined].includes(columns[i].visible)) {
        columns[i].visible = visible
      }

      // Set initial search values from query string
      if (columns[i].name in qs) {
        config.dataTables.searchCols.push({ search: qs[columns[i].name] })
      } else {
        config.dataTables.searchCols.push(null)
      }

      // Uses renderColumn() if defined in render.js
      if (window.renderColumn || null) {
        const render = window.renderColumn(columns[i].name, config)
        if (render) {
          columns[i].render = render
        }
      }
    }
  }

  function _ajaxData (dtp) {
    const msg = '_ajaxData() => Preparing query parameters for AJAX search'
    console.log(`${msg}using dtp object:`)
    console.log(JSON.parse(JSON.stringify(dtp)))

    $('#error').hide()

    if (dtp.search.value) {
      dtp._globalsearch = dtp.search.value
    }

    const _orders = []
    for (let i = 0; i < dtp.order.length; i++) {
      const c = dtp.order[i].column
      const name = dtp.columns[c].name
      _orders.push(dtp.order[i].dir === 'asc' ? name : '-' + name)
    }
    dtp._orders = _orders.join(',')

    dtp._length = dtp.length
    dtp._start = dtp.start
    dtp._draw = dtp.draw

    console.log('_ajaxData() => Deleting non server API keys from dtp object.')
    for (const key in dtp) {
      if (!key.startsWith('_')) {
        delete dtp[key]
      }
    }

    const config = getConfig.config
    const columnObject = array2object(config.dataTables.columns, 'name')
    const qs = parseQueryString()
    for (const [key, val] of Object.entries(qs)) {
      if (!(key in dtp) && (key in columnObject)) {
        const msg = `_ajaxData() => Adding ${key} = '${val}' from query string`
        console.log(`${msg} to dtp object.`)
        dtp[key] = val
      }
    }

    console.log('_ajaxData() => Returning updated dtp object:')
    console.log(dtp)

    console.log('_ajaxData() => Setting query link in DOM.')
    let url = window.location.pathname + 'data/' + '?'
    url += new URLSearchParams(dtp).toString()
    $('#query > a').attr('href', url)
    $('#query').show()

    return dtp
  }

  function _ajaxError (xhr, error, thrown) {
    console.error('AJAX error')
    console.error('xhr: ', xhr)
    console.error('error: ', error)
    console.error('thrown: ', thrown)
    const emsgo = 'An error occurred while loading data'
    let emsg
    if (xhr) {
      const stat = xhr.status || ''
      const text = xhr.statusText || ''
      emsg = `${emsgo}. Status: '${stat}'; Status text: '${text}'`
      if (xhr.responseURL) {
        emsg = `${emsgo} from ${xhr.responseURL}. ${emsg}.`
      }
    }
    if (thrown) {
      emsg += ' Thrown error: ' + thrown + '.'
    }
    try {
      const resp = JSON.parse(xhr.responseText)
      if ('error' in resp) {
        emsg += ` Server response error message: ${resp.error}.`
      }
    } catch (e) {
      console.error('Could not parse xhr.responseText as JSON.')
      console.error(e)
    }
    $(`${tableID}_processing`).hide()
    $('#error').html(emsg + '. See console for details.').show()
  }
}

function createRelatedTablesDropdown (config) {
  const relatedTables = config.dataTablesAdditions.relatedTables || null
  $('#relatedTablesSelect select').empty()
  if (relatedTables && Array.isArray(relatedTables) && relatedTables.length > 0) {
    const options = []
    for (const rt of relatedTables) {
      options.push(`<option value="${rt.path}">${rt.name}</option>`)
    }
    // Determine selected option based on current path
    for (let i = 0; i < relatedTables.length; i++) {
      // Remove trailing slash from path
      const w = window.location.pathname.replace(/\/$/, '')
      if (w.endsWith(relatedTables[i].path)) {
        $('#relatedTablesSelect').append(options).prop('selectedIndex', i + 1)
        $('#relatedTablesSelect').attr('lastSelected', relatedTables[i].path)
        $('#relatedTables').show()
        break
      }
    }
    $('#relatedTablesSelect').on('change', function () {
      const lastSelected = $('#relatedTablesSelect').attr('lastSelected')
      $('#relatedTablesSelect').attr('lastSelected', $(this).val())
      const url = window.location.href.replace(lastSelected, $(this).val())
      if (url) window.location.href = url
    })
  }
}

function createColumnConstraints (tableID, which) {
  const msg = 'createColumnConstraints() => Setting dropdowns and search inputs'
  console.log(msg)

  let parent = `${tableID}_wrapper`
  if ($('.dtfh-floatingparent').length > 0) {
    const msg = 'createColumnConstraints() => Floating header detected. '
    console.log(`${msg}Adding column constraints to floating header.`)
    parent = '.dtfh-floatingparent'
  }
  if (!which) which = 'all'

  let columnOptions = getConfig.config.dataTablesAdditions.columnOptions || {}
  columnOptions = array2object(columnOptions, 'name')

  const table = $(tableID).dataTable()
  const config = getConfig.config
  let visibleIndex = 0
  let showDropdowns = false
  if (config.dataTablesAdditions.columnDropdowns === true) {
    // Set default to showing dropdowns for all columns
    showDropdowns = true
  } else {
    // No need to call createColumnDropdown() to create hidden select
    // to get spacing correct.
    let noDropdowns = true
    for (const name of Object.keys(columnOptions)) {
      if (columnOptions[name].dropdown === true) {
        noDropdowns = false
        break
      }
    }
    if (noDropdowns) showDropdowns = null
  }

  table.api().columns(':visible').every(function () {
    const column = this
    const index = column.index()
    const name = config.dataTables.columns[index].name
    if (which === 'all' || which === 'input') {
      const searchOnKeypress = config.dataTables.columns[index].return === false
      createColumnInput(parent, visibleIndex, name, column, searchOnKeypress)
    }
    if (which === 'all' || which === 'select') {
      let showDropdown = showDropdowns
      if (columnOptions[name] && 'dropdown' in columnOptions[name]) {
        // Override default with column-specific setting
        showDropdown = columnOptions[name].dropdown
      }
      if (config.dataTables.serverSide && showDropdowns !== null) {
        createColumnDropdown(parent, visibleIndex, name, column, showDropdown)
      }
    }
    visibleIndex++
  })
}

function createColumnInput (parent, visibleIndex, name, column, searchOnKeypress) {
  $('#clearAllSearches').hide()

  // Create `input` element
  const element = 'thead tr:eq(0) > th'
  const th = $(`${parent} ${element}`).eq(visibleIndex).empty()
  const attrs = `class="columnSearch" name="${name}"`
  const input = $(`<input ${attrs} type="text" placeholder="Search col."/>`)
  const qsName = getQueryValue(name)

  let title = 'Enter search text and enter to search. '
  title += 'See documentation for search syntax.'

  const val = qsName || ''
  input
    .val(val)
    .attr('title', 'Enter search string and press enter to search')
    .appendTo(th)
    .off('keydown')
    .on('keydown', function (event) {
      if (searchOnKeypress) {
        // Do search on each keypress
        column.search($(this).val()).draw('page')
      } else {
        const keycode = (event.keyCode ? event.keyCode : event.which)
        if (keycode === 13) {
          const msg = 'createColumnInput() => Enter key pressed. Triggering '
          console.log(msg + "search and draw('page').")
          if (th.find('select.columnUniques').length > 0) {
            const select = th.find('select.columnUniques')
            if (select.find('option').length > 0) {
              select.prop('selectedIndex', 0)
            }
          }
          column.search($(this).val()).draw('page')
        }
      }
    })
    .attr('title', title)
    .off('input')
    .on('input', function () {
      if ($(this).val()) {
        clearOneSearch.css('visibility', 'visible')
      } else {
        clearOneSearch.css('visibility', 'hidden')
      }
    })
    .wrap('<div style="white-space: nowrap;"></div>')

  const clearOneSearch = $('<span class="clearOneSearch">✘</span>')
  clearOneSearch.appendTo($(input.parent()))
  clearOneSearch.on('mousedown', function (e) {
    e.preventDefault()
    input.val('')
    clearOneSearch.css('visibility', 'hidden')
    input.focus()
    column.search('').draw('page')
    const thParent = $(this).closest('th')
    const select = thParent.find('select.columnUniques')
    if (select.length > 0) {
      if (select.find('option').length > 0) {
        select.prop('selectedIndex', 0)
      }
    }
  })

  if (val) {
    input.css('background-color', 'yellow')
    clearOneSearch.css('visibility', 'visible')
    $('#clearAllSearches').show()
  }
}

function createColumnDropdown (parent, visibleIndex, name, column, show) {
  if (!createColumnDropdown.cache) {
    createColumnDropdown.cache = {}
  }

  const th = $(`${parent} thead tr:eq(0) > th`).eq(visibleIndex)

  const selectOld = th.find('select.columnUniques')
  selectOld.remove()

  const input = th.find('input.columnSearch')

  const maxLen = 100
  const width = input.outerWidth()
  const title = `Most frequent unique values and (count); max of ${maxLen} shown`
  const attrs = `class="columnUniques" title="${title}" name="${name}"`
  let select = $(`
    <div style="white-space: nowrap;">
      <select ${attrs} style="width: ${width}px;"></select>
      <span style="visibility:hidden">✘</span>
    </div>
  `)
  // let select = $(`<select ${attrs} style="width: ${width}px;"></select>`)
  select.appendTo(th)

  if (show === false) {
    // Place the dropdown in the DOM but do not show so spacing correct.
    return
  }

  const qs = parseQueryString()
  let searches = {}
  for (const [key, val] of Object.entries(qs)) {
    if (!key.startsWith('_')) {
      searches[key] = val
    }
  }

  searches = new URLSearchParams(searches).toString()
  let url = `data/?${searches}&_return=${encodeURIComponent(name)}`
  url += '&_uniques=true&_length=100'

  getOptionHTML(url, setOptionHTML)

  function setOptionHTML (html) {
    if (!html) {
      select.remove()
      return
    }
    if ($(parent).length === 0) {
      // Parent has been removed from DOM, so do not add select.
      return
    }
    select = th.find(`select[name="${name}"]`)
    select.append(html)
    if (qs[name]) {
      select.val(qs[name].replace(/^'/, '').replace(/'$/, ''))
    }
    createColumnDropdown.cache[url] = select.html()
    select.css('visibility', 'visible')
    setChangeEvent(select)
  }

  function getOptionHTML (url, cb) {
    if (createColumnDropdown.cache[url]) {
      // Use cached version
      cb(createColumnDropdown.cache[url])
      return
    }
    // Fetch unique values for this column
    $.ajax({
      url,
      method: 'GET',
      dataType: 'json',
      success: function (data) {
        if (!data[name]) {
          cb(null)
          return
        }
        let html = '<option value="" selected></option>'
        data[name].forEach(function (val) {
          html += `<option value="${val[0]}">${val[0]} (${val[1]})</option>`
        })
        cb(html)
      }
    })
  }

  function setChangeEvent (select) {
    // On select change, update input and trigger search
    select.off('change').on('change', function () {
      console.log('createColumnDropdown => select change event triggered.')
      let val = $(this).val().replace(/'/g, "''")
      if (val === '') {
        console.log('createColumnDropdown => Setting input to empty string.')
        input.val('')
        select.parent().find('span.clearOneSearch').css('visibility', 'hidden')
      } else {
        val = `'${val}'`
        console.log(`createColumnDropdown => Setting input to ${val}.`)
        input.val(val)
        select.parent().find('span.clearOneSearch').css('visibility', 'visible')
      }
      console.log("createColumnDropdown => Triggering search and draw('page').")
      column.search($(this).val()).draw('page')
    })
  }
}

function clearAllSearches (tableID) {
  const qs = parseQueryString()
  for (const key in qs) {
    if (!key.startsWith('_')) {
      delete qs[key]
    }
  }
  location.hash = decodeURIComponent($.param(qs))
  init()
}

function setEvents (tableID) {
  removeEventListener('input', (event) => {
    console.log('Removing input event:', event)
  })
  addEventListener('input', (event) => {
    if (event.target && event.target.className === 'columnSearch') {
      let msg = 'setEvents() => input event. Autocomplete selection event '
      msg += 'on columnSearch input. Showing clearAllSearches button.'
      console.log(msg)
      $('#clearAllSearches').show()
    }
  })

  $('#clearAllSearches').off('click').on('click', clearAllSearches)

  console.log('setEvents() => Setting search.dt.')
  $(tableID).off('search.dt').off('search.dt')
  $(tableID).on('search.dt', function (event) {
    console.log('setEvents() => search.dt triggered')
    updateQueryString('_page', null)
    $(tableID).DataTable().page(0)
    setQueryStringFromSearch()
  })

  $(tableID).on('stateSaveParams.dt', function (e, settings, data) {})

  console.log('setEvents() => Setting page.dt.')
  let pageChanged = false
  $(tableID).off('page.dt').on('page.dt', function () {
    console.log('setEvents() => page.dt triggered')
    const info = $(tableID).DataTable().page.info()
    $('#pageInfo').html('Showing page: ' + info.page + ' of ' + info.pages)
    if (info.page > 0) {
      updateQueryString('_page', info.page + 1)
    } else {
      updateQueryString('_page', null)
    }
    pageChanged = true
  })

  console.log('setEvents() => Setting length.dt.')
  $(tableID).off('length.dt').on('length.dt', function (e, settings, len) {
    console.log('setEvents() => length.dt triggered')
    updateQueryString('_length', len)
    lengthChanged = true
  })

  console.log('setEvents() => Setting preDraw.dt.')
  $(tableID).off('preDraw.dt').on('preDraw.dt', function () {
    console.log('setEvents() => preDraw.dt triggered')
  })

  console.log('setEvents() => Setting draw.dt and triggering it.')
  let _emptyColumns = emptyColumns(tableID)
  $(tableID)
    .off('draw.dt')
    .on('draw.dt', function () {
      console.log('setEvents() => draw.dt triggered.')

      console.log('setEvents() => draw.dt => Calling adjustDOM()')
      adjustDOM(tableID)

      if (pageChanged && getQueryValue('_cols_show') === 'nonempty') {
        const msgo = 'setEvents() => draw.dt => '
        const msg = `${msgo}Page was changed and _cols_show=nonempty. `
        console.log(`${msg}Checking for change in number of empty columns.`)
        pageChanged = false
        const _emptyColumnsNow = emptyColumns(tableID)
        if (_emptyColumns.length === _emptyColumnsNow.length) {
          const msg = `${msgo} Number of empty columns has not changed. Not `
          console.log(`${msg}updating column visibility.`)
          return
        }
        console.log(`${msgo} # of empty columns has changed. Calling init().`)
        _emptyColumns = _emptyColumnsNow
        init()
      }
    })

  console.log('setEvents() => Setting click event for hideEmptyColumns checkbox.')
  $('#hideEmptyColumns').unbind('click')
  $('#hideEmptyColumns').click(function () {
    if ($(this).is(':checked')) {
      console.log('setEvents() => #hideEmptyColumns clicked to checked.')
      updateQueryString('_cols_show', 'nonempty')
      init()
    } else {
      console.log('setEvents() => #hideEmptyColumns clicked to unchecked.')
      updateQueryString('_cols_show', null)
      init()
    }
  })
}

function adjustDOM (tableID) {
  console.log('adjustDOM() => called.')
  const tableInfo = `${tableID}_info`
  const tableLength = `${tableID}_length`
  const tableFilter = `${tableID}_filter`
  const tablePaginate = `${tableID}_paginate`

  console.log('adjustDOM() => Moving global search input.')
  const input = $(`${tableFilter} input`).attr('placeholder', 'Global search')
  $(`${tableFilter} label`).replaceWith(input[0])
  $(`${tableInfo}`).insertAfter(tableFilter)

  console.log('adjustDOM() => Moving global search input.')
  const numCols = $(tableID).DataTable().columns().nodes().length
  const numColsVisible = $(tableID).DataTable().columns(':visible').nodes().length
  let colInfo = ` and all ${numCols} columns`
  if (numCols !== numColsVisible) {
    colInfo = ` and ${numColsVisible} of ${numCols} columns`
  }

  console.log("adjustDOM() => Creating 'Showing ...' string.")
  const info = $(tableID).DataTable().page.info()
  const nRows = info.recordsDisplay.toLocaleString('en-US')
  let txt = `Showing ${parseInt(info.start) + 1}-${parseInt(info.end)} `
  txt += `of ${nRows} rows`
  if (info.recordsDisplay === 0) {
    txt = 'Showing 0 rows'
  }
  if (info.recordsTotal !== info.recordsDisplay) {
    const recordsTotal = info.recordsTotal.toLocaleString('en-US')
    txt += ` (filtered from ${recordsTotal} total)`
  }
  txt += `${colInfo}.`
  $(tableInfo).text(txt)

  console.log('adjustDOM() => Moving pagination controls and hiding if only one page.')
  $(tablePaginate).insertAfter(tableFilter)
  // Hide paging if only one page.
  if ($(tableID).dataTable().api().page.info().pages === 1) {
    console.log('setEvents() => draw.dt => Hiding pagination b/c only one page.')
    $(tablePaginate).hide()
  } else {
    console.log('setEvents() => draw.dt => Showing pagination b/c multiple pages.')
    $(tablePaginate).show()
  }

  console.log('adjustDOM() => Moving length control.')
  const select = $(`${tableLength} select`)
  select.appendTo(tableLength)
  $(`${tableLength} label`).text('Rows per page: ')
  $(`${tableID}_length`).insertBefore('#clearAllSearches')

  console.log('adjustDOM() => Modifying Previous/Next buttons.')
  const prev = $(`${tableID}_previous`)
  prev.attr('title', 'Previous page')
  if (prev.length > 0) {
    const html = prev.html()
    if (html) {
      prev.html(html.replace('Previous', '◀'))
    }
  }
  const next = $(`${tableID}_next`)
  next.attr('title', 'Next page')
  if (next.length > 0) {
    const html = next.html()
    if (html) {
      next.html(html.replace('Next', '▶'))
    }
  }

  if (info.pages > 0) {
    const _page = getQueryValue('_page')
    let msg = `setEvents() => draw.dt =>  _page = ${_page}, info.pages = ${info.pages}. `
    console.log(msg)
    if (_page) {
      if (parseInt(_page) > info.pages) {
        msg += `Setting _page to ${info.pages}`
        console.log(`${msg} and drawing page ${info.pages - 1}.`)
        updateQueryString('_page', info.pages)
        $(tableID).DataTable().page(info.pages - 1).draw('page')
      }
    }
  }

  console.log('adjustDOM() => Moving query link.')
  $(tableInfo).append($('#query'))

  console.log('adjustDOM() => Setting timeout to execute $(window).resize().')
  setTimeout(function () {
    let msg = 'adjustDOM() => Executing $(window).resize() to '
    msg += 'trigger event that causes left column header widths'
    console.log(`${msg} to match the width of the left column body.`)
    $(window).resize()
  }, 0)

  console.log('adjustDOM() finished.')
}

function setColumnWidths (columnOptions) {
  if (!columnOptions) {
    console.log('setColumnWidths() => No columnOptions provided. Returning.')
    return
  }
  columnOptions = array2object(columnOptions, 'name')
  console.log('setColumnWidths() => Setting column widths')
  // columnWidths is an object with keys that are column names
  // and values that are widths, e.g. { 'height': '70em', 'age': '5px' }
  const columnHeads = $(`${tableID} thead tr:eq(0) th`)
  for (let i = 0; i < columnHeads.length; i++) {
    const name = $(columnHeads[i]).attr('name')
    if (columnOptions[name] && columnOptions[name].width) {
      const width = columnOptions[name].width
      console.log(`setColumnWidths() => Setting column ${name} width to ${width}`)
      setColumnWidth(name, width)
    }
  }
  function setColumnWidth (name, width) {
    // Why does only the input width need to be set to get correct width?
    // If widths in tbody are set, run into issues when sticky header is shown.
    // Note that widths will be larger if content in column is wider.
    $('<style>')
      .prop('type', 'text/css')
      .html(`
        thead tr th[name="${name}"] input {
          width: ${width} !important;
        }
      `)
      .appendTo('head')
  }

  function xsetColumnWidth (columnIndex, width) {
    $('<style>')
      .prop('type', 'text/css')
      .html(`
        thead tr th:nth-child(${columnIndex}),
        tbody tr td:nth-child(${columnIndex}) {
          width: ${width} !important;
        }
        thead tr th:nth-child(${columnIndex}) input {
          width: ${width} !important;
        }
      `)
      .appendTo('head')
  }
}

function emptyColumns (tableID, indices) {
  const data = $(tableID).DataTable().rows({ page: 'current' }).data().toArray()
  console.log('emptyColumns() => Data for current page:')
  console.log(data)
  const columnEmpty = []
  for (let r = 0; r < data.length; r++) {
    for (let c = 0; c < data[r].length; c++) {
      if (r === 0) {
        columnEmpty.push(true)
      }
      if (data[r][c] !== null && data[r][c] !== '') {
        columnEmpty[c] = false
      }
    }
  }

  if (indices) {
    const columnEmpty2 = []
    for (let c = 0; c < columnEmpty.length; c++) {
      if (columnEmpty[c]) {
        columnEmpty2.push(c)
      }
    }
    console.log('Computed columnEmpty index array:')
    console.log(columnEmpty2)
    return columnEmpty2
  }

  console.log('emptyColumns() => Computed columnEmpty array:')
  console.log(columnEmpty)

  return columnEmpty
}

function destroy (tableID) {
  console.log('destroy() => Called.')

  const tableWrapper = $(tableID + '_wrapper')
  if (tableWrapper.length > 0) {
    console.log(`destroy() => Destroying table ${tableID}`)
    $('#error').empty()
    $('#tableMetadata').empty()
    $(`${tableID}_length`).remove()
    // https://datatables.net/forums/discussion/comment/190544/#Comment_190544
    $(tableID).DataTable().state.clear()
    $(tableID).DataTable().destroy()
    tableWrapper.remove()
    $(tableID).empty()
    // Remove event handlers added to table, document, and window
    $(tableID).off()
    $(document).off()
    $(window).off()
  } else {
    console.log(`destroy() => No table ${tableID} to destroy.`)
  }
}

function fixStickyHeader (index) {
  if (!index) {
    index = 10
  }
  const parent = `${tableID}_wrapper div.dataTables_scrollHead`
  // Address a bug in DataTables v1 where duplicate header
  // cells appear temporarily during transition to sticky header.
  $(`${parent} thead tr:eq(0) > th:eq(0)`)
    .css('position', 'sticky')
    .css('left', '0px')
    .css('z-index', index)
    .css('background-color', 'white')
  $(`${parent} thead tr:eq(1) > th:eq(0)`)
    .css('position', 'sticky')
    .css('left', '0px')
    .css('z-index', index)
    .css('background-color', 'white')
}

function watchForFloatingHeader () {
  if (!watchForFloatingHeader.enabled) {
    watchForFloatingHeader.enabled = true
  } else {
    return
  }
  let msg = 'watchForFloatingHeader() => Setting up MutationObserver '
  console.log(`${msg}to watch for addition of dtfh-floatingparent element.`)
  const observer = new MutationObserver(function (mutations) {
    mutations.forEach(function (mutation) {
      if (mutation.type === 'childList' && mutation.addedNodes.length > 0) {
        mutation.addedNodes.forEach(function (node) {
          if ($(node).hasClass('dtfh-floatingparent')) {
            msg = 'watchForFloatingHeader() => dtfh-floatingparent '
            console.log(`${msg}was added.`)
            fixStickyHeader(1)
            setTimeout(() => { createColumnConstraints(tableID) }, 1)
          }
        })
      }
    })
  })
  // Observe child additions and in subtrees
  const config = { childList: true, subtree: true }
  // Start observing from the body
  observer.observe(document.body, config)
}

function parseQueryString (hash) {
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
    nvpair[pair[0]] = pair[1]
  })
  return nvpair
}

function setDefaultQueryString (hash) {
  if (hash) {
    const qs = parseQueryString()
    const qsDefault = parseQueryString(hash)
    for (const [key, val] of Object.entries(qsDefault)) {
      if (!(key in qs)) {
        let msg = 'init() => Setting query string parameter '
        msg += `${key} = ${val} from defaultHash: `
        console.log(msg)
        updateQueryString(key, val)
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

function checkQueryString (config) {
  console.log('checkQueryString() => Checking query string for invalid column names.')

  const qs = parseQueryString()
  console.log('checkQueryString() => Query string:')
  console.log(qs)
  const columnObject = array2object(config.dataTables.columns, 'name')

  const msg = "checkQueryString() => what = 'keys'. Checking keys but not "
  console.log(`${msg}values in query string.`)
  let alerted = false
  for (const [key, val] of Object.entries(qs)) {
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
        alert(amsg)
      }
      updateQueryString(key, null)
    }
  }

  console.log("checkQueryString() => what = 'cols'. Checking only _cols parameter in query string.")
  let _cols = getQueryValue('_cols')
  if (!_cols) {
    console.log('checkQueryString() => No _cols in query string. Leaving it.')
    return
  }

  _cols = _cols.split(',')
  let updateHash = false
  for (let i = 0; i < _cols.length; i++) {
    const columnName = _cols[i]
    if (columnName in columnObject) {
      continue
    } else {
      const msg = `checkQueryString() => Column name '${columnName}' not `
      console.log(`${msg}found in column names. Removing it from _cols.`)
      if (!updateHash) {
        let amsg = 'checkQueryString() => Column name in query string not '
        amsg += `found: "${columnName}". Removing it and any other invalid `
        alert(`${amsg}column names from query string.`)
      }
      updateHash = true
      delete _cols[i]
    }
  }
  const columnNames = config.dataTables.columns.map(col => col.name)
  if (updateHash) {
    console.log('checkQueryString() => Updating query string to remove invalid column names.')
    _cols = columnNames.filter(Boolean) // Remove any null/undefined values
    console.log(_cols)
    updateQueryString('_cols', _cols.join(','))
  }
}

function setQueryStringFromSearch () {
  let msg = 'setQueryStringFromSearch() => Stepping through column '
  console.log(`${msg}search inputs.`)
  // Step through column search inputs and update query string
  // Highlight inputs with search values and remove highlight for
  // inputs with no search value.
  let searchValue
  let inputs
  if ($('.dtfh-floatingparent').length > 0) {
    msg = 'setQueryStringFromSearch() => .dtfh-floatingparent found. '
    console.log(`${msg}Using inputs under it.`)
    inputs = $('.dtfh-floatingparent input.columnSearch')
  } else {
    msg = 'setQueryStringFromSearch() => No .dtfh-floatingparent found. '
    console.log(`${msg}Using inputs under .dataTables_scrollHead`)
    inputs = $('.dataTables_scrollHead input.columnSearch')
  }
  msg = 'setQueryStringFromSearch() => Reading '
  console.log(`${msg}${inputs.length} column search inputs.`)
  for (const input of inputs) {
    const name = $(input).attr('name')
    searchValue = $(input).val()
    if (searchValue) {
      let msg = 'setQueryStringFromSearch() => Found search value for column'
      msg += ` '${name}'. Updating query string and highlighting input.`
      $(input).css('background-color', 'yellow')
      msg = 'setQueryStringFromSearch() => Updating query string with '
      msg += `search value for column '${name}' = '${searchValue}'.`
      console.log(msg)
      updateQueryString(name, searchValue)
    } else {
      // console.log(`No search value for column '${name}'.`);
      $(input).css('background-color', '')
      const qs = parseQueryString()
      if (qs[name]) {
        const msg = `setQueryStringFromSearch() => Found ${name} in `
        console.log(`${msg}query string. Removing it from query string.`)
        updateQueryString(name, null)
      }
    }
    const qs = parseQueryString()
    const numSearchKeys = Object.keys(qs).filter(k => !k.startsWith('_')).length
    const msg = 'setQueryStringFromSearch() => There are '
    if (numSearchKeys > 0) {
      console.log(`${msg}${numSearchKeys}search keys in the query string. Showing Clear button.`)
      $('#clearAllSearches').show()
    } else {
      console.log(`${msg}no search terms in the query string. Hiding Clear button.`)
      $('#clearAllSearches').hide()
    }
  }
}

function updateQueryString (name, val) {
  console.log(`updateQueryString() called with name='${name}' and val='${val}'`)
  const qs = parseQueryString()
  if (val === null) {
    console.log(`updateQueryString() => Removing ${name} from query string.`)
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
  location.hash = decodeURIComponent($.param(sortedQs))
}

function array2object (arr, key) {
  if (!key) {
    key = 'name'
  }
  // Create a map from column name to index
  const obj = {}
  for (let i = 0; i < arr.length; i++) {
    obj[arr[i][key]] = arr[i]
  }
  return obj
}
