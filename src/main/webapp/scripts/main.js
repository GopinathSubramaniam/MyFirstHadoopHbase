var file =null;

document.getElementById('file').addEventListener('change', selectFile, false);

function selectFile(ev){
    file = ev.target.files[0]
}

function upload(ev){
    var formData = new FormData();
    formData.append('file', file);
    formData.append("user", JSON.stringify({name: 'Gopi', email:'asdf@asdf', mobile: 'asdsad0', password: 'asdf'}));
    $.ajax({
        url:'/upload/image',
        type: 'POST',
        data: formData,
        enctype: 'multipart/form-data;boundary=gc0p4Jq0M2Yt08jU534c0p',
        cache: false,
        contentType : false,
        processData: false
    }).done(function(res){
        console.log('Remote Address = ', res)
    }).error(function(err){
        console.log('Error ::: ', err);
    });
}