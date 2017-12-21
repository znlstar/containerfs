/* 以下内容为封装函数 */

//操作完成后弹出层，只有确定，参数：提示内容，执行函数（无传null）
function showInfoAlert(content, okFunc) {
    var d = dialog({
        width: 320,
        height: 'auto',
        title: '提示',
        content: content,
        fixed: true,
        okValue: '确定',
        ok: function () {
            if (typeof(okFunc) == 'function') {
                okFunc();
            }
        }
    });

    d.showModal();
}

//操作完成后弹出层，确定和取消，参数：提示内容，确定函数（无传null）
function showInfoAlertSelect(content, okFunc) {
    var d = dialog({
        width: 320,
        height: 'auto',
        title: '提示',
        content: content,
        fixed: true,
        okValue: '确定',
        ok: function () {
            if (typeof(okFunc) == 'function') {
                okFunc();
            }
        },
        cancelValue: '取消',
        cancel: function () {
        }
    });

    d.showModal();
}