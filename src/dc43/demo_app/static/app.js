const { createApp } = Vue;

createApp({
  data() {
    return {
      view: 'datasets',
      datasets: [],
      contracts: [],
      datasetDetail: null,
      contractDetail: null,
    };
  },
  methods: {
    refresh() {
      fetch('/api/datasets').then(r => r.json()).then(d => this.datasets = d);
      fetch('/api/contracts').then(r => r.json()).then(d => this.contracts = d);
    },
    openDataset(ver) {
      fetch(`/api/datasets/${ver}`).then(r => r.json()).then(d => { this.datasetDetail = d; this.view = 'dataset-detail'; });
    },
    openContract(id, ver) {
      fetch(`/api/contracts/${id}/${ver}`).then(r => r.json()).then(d => { this.contractDetail = d; this.view = 'contract-detail'; });
    },
    // validation endpoint removed
  },
  mounted() {
    this.refresh();
  }
}).mount('#app');
