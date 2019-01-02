<jsp:include page="../cdg_header.jsp" />
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<script>
	function jsonconstruct() {
		var x = '{"header":{"user":"info@clouddatagrid.com","service_account":"Extraction_CDG_UK",'+
			'"reservoir_id":"R0001","event_time":"today"},"body":{"data":{"src_sys_id":"'
				+ document.getElementById("src_sys_id").value + '","data_path":"'
				+ document.getElementById("data_path").value+'"';
		document.getElementById('x').value = x;
		//alert(x);
		//console.log(x);
		document.getElementById('DataDetails').submit();
	}
	function funccheck(val) {
		if (val == 'create') {
			window.location.reload();
		} else if (val == 'edit') {
			$("#src_sys_id").val("");
		}
	}
	$(document).ready(function() {
		$("#src_sys_id").change(function() {
		var src_sys_id = $(this).val();
		var src_val = document.getElementById("src_val").value;
		$.post('${pageContext.request.contextPath}/extraction/DataDetailsUnix1',
		{
			src_sys_id : src_sys_id,
			src_val : src_val
		}, function(data) {
			$('#dyn').html(data)
		});
	});
});
</script>

<div class="main-panel">
	<div class="content-wrapper">
		<div class="row">
			<div class="col-12 grid-margin stretch-card">
				<div class="card">
					<div class="card-body">
						<h4 class="card-title">Data Extraction</h4>
						<p class="card-description">Data Details</p>
						<%
							if (request.getAttribute("successString") != null) {
						%>
						<p class="text-success h4">${successString}</p>
						<%
							}
						%>
						<%
							if (request.getAttribute("errorString") != null) {
						%>
						<p class="text-danger h4">${errorString}</p>
						<%
							}
						%>
						<form class="forms-sample" id="DataDetails" name="DataDetails"
							method="POST"
							action="${pageContext.request.contextPath}/extraction/DataDetailsUnix2"
							enctype="multipart/form-data">
							<input type="hidden" name="x" id="x" value=""> <input
								type="hidden" name="src_val" id="src_val" value="${src_val}">

							<div class="form-group row">
								<div class="col-sm-4">
									<div class="form-check form-check-info">
										<label class="form-check-label"> <input type="radio"
											class="form-check-input" name="radio" id="radio1"
											checked="checked" value="create"
											onclick="funccheck(this.value)"> Create
										</label>
									</div>
								</div>
								<div class="col-sm-4">
									<div class="form-check form-check-info">
										<label class="form-check-label"> <input type="radio"
											class="form-check-input" name="radio" id="radio2"
											value="edit" onclick="funccheck(this.value)"> Edit
										</label>
									</div>
								</div>
							</div>
							<div class="form-group">
								<label>Source Feed Name *</label> <select name="src_sys_id"
									id="src_sys_id" class="form-control">
									<option value="" selected disabled>Source Feed Name
										...</option>
									<c:forEach items="${src_sys_val}" var="src_sys_val">
										<option value="${src_sys_val.src_sys_id}">${src_sys_val.src_unique_name}</option>
									</c:forEach>
								</select>
							</div>
							<div id="dyn"></div>
							<div class="form-group">
							<a
								href='https://storage.cloud.google.com/cdg_on_prem/file_details.xlsx'
								target="_blank">Download the File Details Template here!</a>
								<input type="file" id="file" name="file"
									class="file-upload-default">
								<div class="input-group col-xs-12">
									<input type="text"
										class="form-control form-control1 file-upload-info"
										name="file" disabled placeholder="Upload File Details">
									<span class="input-group-append">
										<button
											class="file-upload-browse btn-rounded btn btn-gradient-info"
											type="button">Upload File Details</button>
									</span>
								</div>
							</div>
							<div class="form-group">
							 <a
								href='https://storage.cloud.google.com/cdg_on_prem/field_details.xlsx'
								target="_blank">Download the Field Details Template here!</a>
								<input type="file" id="field" name="field"
									class="file-upload-default">
								<div class="input-group col-xs-12">
									<input type="text"
										class="form-control form-control1 file-upload-info"
										name="field" disabled placeholder="Upload File Details">
									<span class="input-group-append">
										<button
											class="file-upload-browse btn-rounded btn btn-gradient-info"
											type="button">Upload Field Details</button>
									</span>
								</div>
							</div>
							<button onclick="jsonconstruct();"
	class="btn btn-rounded btn-gradient-info mr-2">Save</button>
						</form>
					</div>
				</div>
			</div>
		</div>
		<script src="../../assets/js/file-upload.js"></script>
		<jsp:include page="../cdg_footer.jsp" />